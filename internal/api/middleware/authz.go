package middleware

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/logs"
	"github.com/calypr/syfon/client/request"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/sync/singleflight"

)

type AuthzMiddleware struct {
	logger        *slog.Logger
	mode          string
	basicUser     string
	basicPass     string
	mock          mockAuthConfig
	cache         *authzCache
	sf            singleflight.Group
	pluginManager pluginManagerInterface // interface for testability
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
	m := &AuthzMiddleware{
		logger:    logger,
		mode:      strings.ToLower(strings.TrimSpace(mode)),
		basicUser: basicUser,
		basicPass: basicPass,
		mock:      loadMockAuthConfigFromEnv(),
		cache:     cache,
	}
	// TODO: Make plugin path configurable; for now, use default path or env var
	pluginPath := os.Getenv("SYFON_AUTHZ_PLUGIN_PATH")
	if pluginPath != "" {
		pm, err := NewPluginManager(pluginPath)
		if err == nil {
		m.pluginManager = pm
		}
	}
	return m
}

// FiberMiddleware returns a fiber middleware that extracts the token and fetches user info.
func (m *AuthzMiddleware) FiberMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx, authHeader := m.prepareRequestContext(c)
		if m.mode != "gen3" {
			return m.handleLocalAuth(c, ctx, authHeader)
		}
		return m.handleGen3Auth(c, ctx, authHeader)
	}
}

func (m *AuthzMiddleware) prepareRequestContext(c fiber.Ctx) (context.Context, string) {
	ctx := context.WithValue(c.Context(), common.AuthHeaderPresentKey, false)
	ctx = context.WithValue(ctx, common.AuthModeKey, m.mode)
	return ctx, c.Get(fiber.HeaderAuthorization)
}

func (m *AuthzMiddleware) handleLocalAuth(c fiber.Ctx, ctx context.Context, authHeader string) error {
	if m.basicUser != "" || m.basicPass != "" {
		if err := validateBasicAuth(authHeader, m.basicUser, m.basicPass); err != nil {
			c.Set(fiber.HeaderWWWAuthenticate, `Basic realm="syfon"`)
			return c.SendStatus(fiber.StatusUnauthorized)
		}
	}
	c.SetContext(ctx)
	return c.Next()
}

func validateBasicAuth(authHeader, expectedUser, expectedPass string) error {
	if authHeader == "" || !strings.HasPrefix(strings.ToLower(authHeader), "basic ") {
		return fmt.Errorf("missing basic auth header")
	}

	payload := authHeader[len("basic "):]
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return fmt.Errorf("decode basic auth header: %w", err)
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 ||
		subtle.ConstantTimeCompare([]byte(parts[0]), []byte(expectedUser)) != 1 ||
		subtle.ConstantTimeCompare([]byte(parts[1]), []byte(expectedPass)) != 1 {
		return fmt.Errorf("invalid basic auth credentials")
	}
	return nil
}

func (m *AuthzMiddleware) handleGen3Auth(c fiber.Ctx, ctx context.Context, authHeader string) error {
	if m.mock.Enabled {
		return m.handleMockAuth(c, ctx)
	}
	if authHeader == "" {
		c.SetContext(ctx)
		return c.Next()
	}

	ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
	tokenString, err := extractBearerLikeToken(authHeader)
	if err != nil {
		m.logger.Debug("failed to parse authorization header", "error", err)
		c.SetContext(ctx)
		return c.Next()
	}

	if m.pluginManager == nil {
		m.logger.Debug("authorization plugin manager unavailable")
		return c.SendStatus(fiber.StatusUnauthorized)
	}

	// Extract plugin input fields from request/context
	input := buildAuthorizationInput(c, ctx, tokenString)
	output, err := m.pluginManager.Authorize(ctx, input)
	if err != nil {
		m.logger.Debug("authorization failed", "error", err)
		return c.SendStatus(fiber.StatusUnauthorized)
	}
	if !output.Allow {
		return c.SendStatus(fiber.StatusForbidden)
	}

	// Map plugin output to resources/privileges for context
	resources, privileges := mapPluginOutputToContext(output)
	return m.populateAuthContextAndNext(c, ctx, resources, privileges)
}

// --- Plugin input/output mapping helpers ---

func buildAuthorizationInput(c fiber.Ctx, ctx context.Context, tokenString string) *AuthorizationInput {
	claims := extractClaimsFromToken(tokenString)
	return &AuthorizationInput{
		RequestID: extractRequestID(c),
		Subject:   extractSubjectFromClaims(claims),
		Action:    c.Method(),
		Resource:  c.Path(),
		Claims:    claims,
		Metadata:  extractRequestMetadata(c, ctx),
	}
}

func extractClaimsFromToken(tokenString string) map[string]interface{} {
	claims := map[string]interface{}{}
	token, _ := jwt.Parse(tokenString, nil)
	if token != nil {
		if mapClaims, ok := token.Claims.(jwt.MapClaims); ok {
			for k, v := range mapClaims {
				claims[k] = v
			}
		}
	}
	return claims
}

func extractRequestID(c fiber.Ctx) string {
	rid := c.Get("X-Request-Id")
	if rid == "" {
		if v := c.Locals("requestid"); v != nil {
			if s, ok := v.(string); ok {
				rid = s
			}
		}
	}
	if rid == "" {
		rid = fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return rid
}

func extractSubjectFromClaims(claims map[string]interface{}) string {
	if sub, ok := claims["sub"].(string); ok {
		return sub
	}
	return ""
}

func extractRequestMetadata(c fiber.Ctx, ctx context.Context) map[string]interface{} {
	meta := map[string]interface{}{
		"ip":         c.IP(),
		"user_agent": c.Get(fiber.HeaderUserAgent),
		"host":       c.Hostname(),
		"url":        c.OriginalURL(),
	}
	return meta
}

func mapPluginOutputToContext(output *AuthorizationOutput) ([]string, map[string]map[string]bool) {
	resources := []string{}
	privileges := map[string]map[string]bool{}
	if output.Obligations != nil {
		if res, ok := output.Obligations["resources"].([]interface{}); ok {
			for _, r := range res {
				if s, ok := r.(string); ok {
					resources = append(resources, s)
				}
			}
		}
		if priv, ok := output.Obligations["privileges"].(map[string]interface{}); ok {
			for res, methods := range priv {
				if arr, ok := methods.([]interface{}); ok {
					privileges[res] = map[string]bool{}
					for _, m := range arr {
						if ms, ok := m.(string); ok {
							privileges[res][ms] = true
						}
					}
				}
			}
		}
	}
	return resources, privileges
}

func (m *AuthzMiddleware) handleMockAuth(c fiber.Ctx, ctx context.Context) error {
	if m.mock.RequireAuthHeader && !authz.HasAuthHeader(ctx) {
		c.SetContext(ctx)
		return c.Next()
	}
	// In mock mode, mark auth header as present so gen3 authorization checks
	// in service/DB layers evaluate injected privileges.
	if !authz.HasAuthHeader(ctx) {
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
	}
	resources, privileges := m.mockAuthPrivileges()
	return m.populateAuthContextAndNext(c, ctx, resources, privileges)
}

func (m *AuthzMiddleware) mockAuthPrivileges() ([]string, map[string]map[string]bool) {
	resources := append([]string(nil), m.mock.Resources...)
	privs := make(map[string]map[string]bool, len(resources))
	for _, resource := range resources {
		methods := make(map[string]bool, len(m.mock.Methods))
		for _, method := range m.mock.Methods {
			methods[method] = true
		}
		privs[resource] = methods
	}
	return resources, privs
}

type authFetchResult struct {
	resources  []string
	privileges map[string]map[string]bool
	negative   bool
}

func (m *AuthzMiddleware) resolveTokenAuth(ctx context.Context, tokenString string) authFetchResult {
	cacheKey := tokenCacheKey(tokenString)
	if resources, privileges, negative, ok := m.cachedAuthResult(cacheKey); ok {
		return authFetchResult{
			resources:  resources,
			privileges: privileges,
			negative:   negative,
		}
	}

	v, _, _ := m.sf.Do(cacheKey, func() (interface{}, error) {
		return m.fetchTokenAuth(ctx, tokenString)
	})
	res, _ := v.(authFetchResult)
	if m.cache != nil {
		m.cache.set(cacheKey, res.resources, res.privileges, res.negative)
	}
	return res
}

func (m *AuthzMiddleware) cachedAuthResult(cacheKey string) ([]string, map[string]map[string]bool, bool, bool) {
	if m.cache == nil {
		return nil, nil, false, false
	}
	return m.cache.get(cacheKey)
}

func (m *AuthzMiddleware) fetchTokenAuth(ctx context.Context, tokenString string) (interface{}, error) {
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
	reqClient := request.NewRequestor(gen3Logger, cred, nil, apiEndpoint, "syfon-server", nil)

	// 3. Fetch user info (privileges)
	privs, err := fetchPrivileges(ctx, reqClient, cred)
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
}

func (m *AuthzMiddleware) populateAuthContextAndNext(c fiber.Ctx, ctx context.Context, resources []string, privileges map[string]map[string]bool) error {
	ctx = context.WithValue(ctx, common.UserAuthzKey, resources)
	ctx = context.WithValue(ctx, common.UserPrivilegesKey, privileges)
	c.SetContext(ctx)
	return c.Next()
}

func fetchPrivileges(ctx context.Context, reqClient request.Requester, cred *conf.Credential) (map[string]any, error) {
	var data map[string]any
	err := reqClient.Do(ctx, http.MethodGet, "/user/user", nil, &data)
	if err != nil {
		return nil, fmt.Errorf("request user info: %w", err)
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
	// SECURITY FIX CRIT-1: Verify JWT signature cryptographically
	// This is the PRIMARY defense against token forgery attacks

	// 1. Parse the JWT header to get KID and ISS claim
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"RS256", "RS384", "RS512"}))
	var claims jwt.MapClaims

	token, err := parser.ParseWithClaims(tokenString, &claims, func(token *jwt.Token) (interface{}, error) {
		// CRITICAL: Verify the signing method is RSA (not "none" or symmetric)
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v (expected RSA)", token.Header["alg"])
		}

		// Extract KID from header
		kid, ok := token.Header["kid"].(string)
		if !ok || kid == "" {
			return nil, fmt.Errorf("missing KID in token header")
		}

		// Extract ISS claim to determine which JWKS endpoint to use
		iss, ok := claims["iss"].(string)
		if !ok || iss == "" {
			return nil, fmt.Errorf("missing or invalid 'iss' claim in token")
		}

		origin, err := normalizeIssuerOrigin(iss)
		if err != nil {
			return nil, fmt.Errorf("invalid issuer URL: %w", err)
		}

		// SECURITY FIX: Validate issuer against allowlist BEFORE fetching keys.
		// Matching is done on normalized origin (scheme://host), not raw iss text.
		if !isIssuerAllowed(origin) {
			return nil, fmt.Errorf("issuer %q not in allowed list", iss)
		}

		// SECURITY FIX: Enforce HTTPS-only for JWKS fetching
		jwksURL := origin + "/.well-known/jwks.json"
		if !strings.HasPrefix(jwksURL, "https://") {
			return nil, fmt.Errorf("JWKS endpoint must use HTTPS, got: %s", jwksURL)
		}

		// Fetch and cache JWKS keys
		cache := NewJWKSCache(jwksURL, 15*time.Minute)
		if err := cache.FetchKeys(); err != nil {
			return nil, fmt.Errorf("fetch JWKS: %w", err)
		}

		// Get the public key for this KID
		publicKey, err := cache.GetKey(kid)
		if err != nil {
			return nil, fmt.Errorf("key not found in JWKS (kid=%s): %w", kid, err)
		}

		return publicKey, nil
	})

	if err != nil {
		return "", 0, fmt.Errorf("JWT signature verification failed: %w", err)
	}

	// Verify the token is valid
	if !token.Valid {
		return "", 0, fmt.Errorf("invalid token")
	}

	// Extract claims after successful verification
	iss, ok := claims["iss"].(string)
	if !ok || iss == "" {
		return "", 0, fmt.Errorf("missing 'iss' claim")
	}

	origin, err := normalizeIssuerOrigin(iss)
	if err != nil {
		return "", 0, fmt.Errorf("failed to normalize issuer URL: %w", err)
	}
	if !strings.HasPrefix(origin, "https://") {
		return "", 0, fmt.Errorf("issuer URL must use https scheme, got %q", iss)
	}

	endpoint = origin

	exp, _ = claims["exp"].(float64)

	return endpoint, exp, nil
}

// isIssuerAllowed checks if an issuer URL is in the allowed list.
// The allowlist is configured via DRS_ALLOWED_ISSUERS (comma-separated URLs).
func isIssuerAllowed(iss string) bool {
	issuerOrigin, err := normalizeIssuerOrigin(iss)
	if err != nil {
		return false
	}

	allowlist := splitCSV(os.Getenv("DRS_ALLOWED_ISSUERS"))
	if len(allowlist) == 0 {
		// If no allowlist is configured, reject all issuers
		return false
	}
	for _, allowed := range allowlist {
		allowedOrigin, err := normalizeIssuerOrigin(allowed)
		if err != nil {
			continue
		}
		if issuerOrigin == allowedOrigin {
			return true
		}
	}
	return false
}

func normalizeIssuerOrigin(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("issuer must include scheme and host")
	}
	return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host), nil
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

type pluginManagerInterface interface {
	Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error)
}

