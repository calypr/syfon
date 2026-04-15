package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/db/core"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/sync/singleflight"
)

type AuthzMiddleware struct {
	logger    *slog.Logger
	mode      string
	basicUser string
	basicPass string
	mock      mockAuthConfig
	cache     *authzCache
	sf        singleflight.Group
}

type mockAuthConfig struct {
	Enabled           bool
	RequireAuthHeader bool
	Resources         []string
	Methods           []string
}

type authCacheConfig struct {
	Enabled      bool
	TTL          time.Duration
	NegativeTTL  time.Duration
	MaxEntries   int
	CleanupEvery time.Duration
}

type authzCache struct {
	cfg authCacheConfig

	mu      sync.RWMutex
	entries map[string]authzCacheEntry
}

type authzCacheEntry struct {
	resources  []string
	privileges map[string]map[string]bool
	negative   bool
	expiresAt  time.Time
}

func NewAuthzMiddleware(logger *slog.Logger, mode, basicUser, basicPass string) *AuthzMiddleware {
	cfg := loadAuthCacheConfigFromEnv()
	var cache *authzCache
	if cfg.Enabled {
		cache = newAuthzCache(cfg)
	}
	return &AuthzMiddleware{
		logger:    logger,
		mode:      strings.ToLower(strings.TrimSpace(mode)),
		basicUser: basicUser,
		basicPass: basicPass,
		mock:      loadMockAuthConfigFromEnv(),
		cache:     cache,
	}
}

// FiberMiddleware returns a fiber middleware that extracts the token and fetches user info.
func (m *AuthzMiddleware) FiberMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx := context.WithValue(c.Context(), core.AuthHeaderPresentKey, false)
		ctx = context.WithValue(ctx, core.AuthModeKey, m.mode)
		
		authHeader := c.Get(fiber.HeaderAuthorization)

		if m.mode != "gen3" {
			if m.basicUser != "" || m.basicPass != "" {
				if authHeader == "" || !strings.HasPrefix(strings.ToLower(authHeader), "basic ") {
					c.Set(fiber.HeaderWWWAuthenticate, `Basic realm="syfon"`)
					return c.SendStatus(fiber.StatusUnauthorized)
				}
				
				payload := authHeader[len("basic "):]
				decoded, err := base64.StdEncoding.DecodeString(payload)
				if err != nil {
					c.Set(fiber.HeaderWWWAuthenticate, `Basic realm="syfon"`)
					return c.SendStatus(fiber.StatusUnauthorized)
				}
				parts := strings.SplitN(string(decoded), ":", 2)
				if len(parts) != 2 ||
					subtle.ConstantTimeCompare([]byte(parts[0]), []byte(m.basicUser)) != 1 ||
					subtle.ConstantTimeCompare([]byte(parts[1]), []byte(m.basicPass)) != 1 {
					c.Set(fiber.HeaderWWWAuthenticate, `Basic realm="syfon"`)
					return c.SendStatus(fiber.StatusUnauthorized)
				}
			}
			c.SetContext(ctx)
			return c.Next()
		}
		if m.mock.Enabled {
			if m.mock.RequireAuthHeader && !core.HasAuthHeader(ctx) {
				c.SetContext(ctx)
				return c.Next()
			}
			// In mock mode, mark auth header as present so gen3 authorization checks
			// in service/DB layers evaluate injected privileges.
			if !core.HasAuthHeader(ctx) {
				ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
			}
			resources := append([]string(nil), m.mock.Resources...)
			privs := make(map[string]map[string]bool, len(resources))
			for _, resource := range resources {
				methods := make(map[string]bool, len(m.mock.Methods))
				for _, method := range m.mock.Methods {
					methods[method] = true
				}
				privs[resource] = methods
			}
			ctx = context.WithValue(ctx, core.UserAuthzKey, resources)
			ctx = context.WithValue(ctx, core.UserPrivilegesKey, privs)
			c.SetContext(ctx)
			return c.Next()
		}
		if authHeader == "" {
			c.SetContext(ctx)
			return c.Next()
		}
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		tokenString, err := extractBearerLikeToken(authHeader)
		if err != nil {
			m.logger.Debug("failed to parse authorization header", "error", err)
			c.SetContext(ctx)
			return c.Next()
		}

		cacheKey := tokenCacheKey(tokenString)
		if m.cache != nil {
			if resources, privileges, negative, ok := m.cache.get(cacheKey); ok {
				if negative {
					c.SetContext(ctx)
					return c.Next()
				}
				ctx = context.WithValue(ctx, core.UserAuthzKey, resources)
				ctx = context.WithValue(ctx, core.UserPrivilegesKey, privileges)
				c.SetContext(ctx)
				return c.Next()
			}
		}

		type authFetchResult struct {
			resources  []string
			privileges map[string]map[string]bool
			negative   bool
		}

		v, _, _ := m.sf.Do(cacheKey, func() (interface{}, error) {
			// 1. Discover Fence API endpoint from token 'iss' claim
			apiEndpoint, _, err := m.parseToken(tokenString)
			if err != nil {
				m.logger.Debug("failed to parse token", "error", err)
				return authFetchResult{negative: true}, nil
			}

			// 2. Initialize request client for authz lookup.
			cred := &conf.Credential{
				AccessToken: tokenString,
				APIEndpoint: apiEndpoint,
			}

			// We use a no-op gen3 logger for the request client to avoid unnecessary side effects in middleware
			gen3Logger := logs.NewGen3Logger(m.logger, "", "syfon")
			reqClient := request.NewRequestInterface(gen3Logger, cred, nil, apiEndpoint, "syfon-server", nil)

			// 3. Fetch user info (privileges)
			privs, err := fetchPrivileges(c.Context(), reqClient, cred)
			if err != nil {
				m.logger.Debug("failed to check privileges with internal auth", "error", err)
				return authFetchResult{negative: true}, nil
			}

			// 4. Map privileges to authorized resources + methods
			authorizedResources, privileges := m.extractPrivileges(privs)
			return authFetchResult{
				resources:  authorizedResources,
				privileges: privileges,
				negative:   false,
			}, nil
		})
		res, _ := v.(authFetchResult)

		if m.cache != nil {
			m.cache.set(cacheKey, res.resources, res.privileges, res.negative)
		}

		if res.negative {
			c.SetContext(ctx)
			return c.Next()
		}

		ctx = context.WithValue(ctx, core.UserAuthzKey, res.resources)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, res.privileges)

		c.SetContext(ctx)
		return c.Next()
	}
}

func fetchPrivileges(ctx context.Context, reqClient request.RequestInterface, cred *conf.Credential) (map[string]any, error) {
	resp, err := reqClient.Do(ctx, &request.RequestBuilder{
		Url:    strings.TrimRight(cred.APIEndpoint, "/") + "/user/user",
		Method: http.MethodGet,
		Token:  cred.AccessToken,
	})
	if err != nil {
		return nil, fmt.Errorf("request user info: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read user info response: %w", err)
	}

	var data map[string]any
	if err := json.Unmarshal(bodyBytes, &data); err != nil {
		return nil, fmt.Errorf("parse user info response: %w", err)
	}

	resourceAccess, ok := data["authz"].(map[string]any)
	if !ok || len(resourceAccess) == 0 {
		resourceAccess, ok = data["project_access"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("no authz/project_access found in user response")
		}
	}
	return resourceAccess, nil
}

func newAuthzCache(cfg authCacheConfig) *authzCache {
	return &authzCache{
		cfg:     cfg,
		entries: make(map[string]authzCacheEntry, cfg.MaxEntries),
	}
}

func (c *authzCache) get(key string) ([]string, map[string]map[string]bool, bool, bool) {
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, nil, false, false
	}
	if now.After(entry.expiresAt) {
		c.mu.Lock()
		// Re-check under write lock to avoid deleting refreshed entries.
		if curr, ok := c.entries[key]; ok && now.After(curr.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, nil, false, false
	}
	return cloneStrings(entry.resources), clonePrivMap(entry.privileges), entry.negative, true
}

func (c *authzCache) set(key string, resources []string, privileges map[string]map[string]bool, negative bool) {
	ttl := c.cfg.TTL
	if negative {
		ttl = c.cfg.NegativeTTL
	}
	if ttl <= 0 {
		return
	}
	entry := authzCacheEntry{
		resources:  cloneStrings(resources),
		privileges: clonePrivMap(privileges),
		negative:   negative,
		expiresAt:  time.Now().Add(ttl),
	}

	c.mu.Lock()
	c.entries[key] = entry
	if len(c.entries) > c.cfg.MaxEntries {
		c.evictExpiredOrOldestLocked()
	}
	c.mu.Unlock()
}

func (c *authzCache) evictExpiredOrOldestLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expiresAt) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) <= c.cfg.MaxEntries {
		return
	}

	type kv struct {
		key string
		exp time.Time
	}
	all := make([]kv, 0, len(c.entries))
	for k, v := range c.entries {
		all = append(all, kv{key: k, exp: v.expiresAt})
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].exp.Before(all[j].exp)
	})

	toDrop := len(c.entries) - c.cfg.MaxEntries
	for i := 0; i < toDrop && i < len(all); i++ {
		delete(c.entries, all[i].key)
	}
}

func loadAuthCacheConfigFromEnv() authCacheConfig {
	cfg := authCacheConfig{
		Enabled:      parseBoolEnv("DRS_AUTH_CACHE_ENABLED", true),
		TTL:          parseDurationSecondsEnv("DRS_AUTH_CACHE_TTL_SECONDS", 45),
		NegativeTTL:  parseDurationSecondsEnv("DRS_AUTH_CACHE_NEGATIVE_TTL_SECONDS", 8),
		MaxEntries:   parseIntEnv("DRS_AUTH_CACHE_MAX_ENTRIES", 20000),
		CleanupEvery: parseDurationSecondsEnv("DRS_AUTH_CACHE_CLEANUP_SECONDS", 60),
	}
	if cfg.MaxEntries < 1 {
		cfg.MaxEntries = 1
	}
	return cfg
}

func parseDurationSecondsEnv(name string, defSeconds int) time.Duration {
	v := parseIntEnv(name, defSeconds)
	if v < 0 {
		v = defSeconds
	}
	return time.Duration(v) * time.Second
}

func parseIntEnv(name string, def int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	var v int
	_, err := fmt.Sscanf(raw, "%d", &v)
	if err != nil {
		return def
	}
	return v
}

func tokenCacheKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func clonePrivMap(in map[string]map[string]bool) map[string]map[string]bool {
	if len(in) == 0 {
		return map[string]map[string]bool{}
	}
	out := make(map[string]map[string]bool, len(in))
	for k, methods := range in {
		if methods == nil {
			out[k] = map[string]bool{}
			continue
		}
		mm := make(map[string]bool, len(methods))
		for mk, mv := range methods {
			mm[mk] = mv
		}
		out[k] = mm
	}
	return out
}

func loadMockAuthConfigFromEnv() mockAuthConfig {
	enabled := parseBoolEnv("DRS_AUTH_MOCK_ENABLED", false)
	if !enabled {
		return mockAuthConfig{}
	}
	resources := splitCSV(os.Getenv("DRS_AUTH_MOCK_RESOURCES"))
	if len(resources) == 0 {
		resources = []string{"/data_file"}
	}
	methods := splitCSV(os.Getenv("DRS_AUTH_MOCK_METHODS"))
	if len(methods) == 0 {
		methods = []string{"*"}
	}
	return mockAuthConfig{
		Enabled:           true,
		RequireAuthHeader: parseBoolEnv("DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER", false),
		Resources:         resources,
		Methods:           methods,
	}
}

func parseBoolEnv(name string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return def
	}
}

func splitCSV(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}

func (m *AuthzMiddleware) parseToken(tokenString string) (endpoint string, exp float64, err error) {
	parser := jwt.NewParser()
	claims := jwt.MapClaims{}
	_, _, err = parser.ParseUnverified(tokenString, claims)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse token: %w", err)
	}

	iss, ok := claims["iss"].(string)
	if !ok || iss == "" {
		return "", 0, fmt.Errorf("token missing 'iss' claim")
	}

	parsedURL, err := url.Parse(iss)
	if err != nil {
		return "", 0, fmt.Errorf("failed to parse 'iss' URL: %w", err)
	}

	endpoint = fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host)

	exp, _ = claims["exp"].(float64)

	return endpoint, exp, nil
}

func extractBearerLikeToken(authHeader string) (string, error) {
	trimmed := strings.TrimSpace(authHeader)
	if strings.HasPrefix(strings.ToLower(trimmed), "bearer ") {
		token := strings.TrimSpace(trimmed[len("Bearer "):])
		if token == "" {
			return "", fmt.Errorf("empty bearer token")
		}
		return token, nil
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "basic ") {
		payload := strings.TrimSpace(trimmed[len("Basic "):])
		if payload == "" {
			return "", fmt.Errorf("empty basic auth payload")
		}
		decoded, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return "", fmt.Errorf("invalid basic auth payload: %w", err)
		}
		parts := strings.SplitN(string(decoded), ":", 2)
		if len(parts) != 2 {
			return "", fmt.Errorf("malformed basic auth credentials")
		}
		token := strings.TrimSpace(parts[1])
		if token == "" {
			return "", fmt.Errorf("empty basic auth password token")
		}
		return token, nil
	}
	return "", fmt.Errorf("unsupported authorization scheme")
}

func (m *AuthzMiddleware) extractPrivileges(privs map[string]any) ([]string, map[string]map[string]bool) {
	resources := make([]string, 0, len(privs))
	out := make(map[string]map[string]bool, len(privs))
	for path, raw := range privs {
		resources = append(resources, path)
		methods := map[string]bool{}
		entries, ok := raw.([]any)
		if !ok {
			out[path] = methods
			continue
		}
		for _, entry := range entries {
			mm, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			service, _ := mm["service"].(string)
			if service != "" && service != "indexd" && service != "drs" && service != "*" {
				continue
			}
			method, _ := mm["method"].(string)
			if method == "" {
				continue
			}
			methods[method] = true
		}
		out[path] = methods
	}
	return resources, out
}
