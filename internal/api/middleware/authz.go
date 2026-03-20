package middleware

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/calypr/data-client/conf"
	"github.com/calypr/data-client/fence"
	"github.com/calypr/data-client/logs"
	"github.com/calypr/data-client/request"
	"github.com/calypr/drs-server/db/core"
	"github.com/golang-jwt/jwt/v5"
)

type AuthzMiddleware struct {
	logger    *slog.Logger
	mode      string
	basicUser string
	basicPass string
	mock      mockAuthConfig
}

type mockAuthConfig struct {
	Enabled           bool
	RequireAuthHeader bool
	Resources         []string
	Methods           []string
}

func NewAuthzMiddleware(logger *slog.Logger, mode, basicUser, basicPass string) *AuthzMiddleware {
	return &AuthzMiddleware{
		logger:    logger,
		mode:      strings.ToLower(strings.TrimSpace(mode)),
		basicUser: basicUser,
		basicPass: basicPass,
		mock:      loadMockAuthConfigFromEnv(),
	}
}

// Middleware returns a mux middleware that extracts the token and fetches user info.
func (m *AuthzMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), core.AuthHeaderPresentKey, false)
		ctx = context.WithValue(ctx, core.AuthModeKey, m.mode)
		if m.mode != "gen3" {
			if m.basicUser != "" || m.basicPass != "" {
				user, pass, ok := r.BasicAuth()
				if !ok ||
					subtle.ConstantTimeCompare([]byte(user), []byte(m.basicUser)) != 1 ||
					subtle.ConstantTimeCompare([]byte(pass), []byte(m.basicPass)) != 1 {
					w.Header().Set("WWW-Authenticate", `Basic realm="drs-server"`)
					http.Error(w, "Unauthorized", http.StatusUnauthorized)
					return
				}
			}
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		if m.mock.Enabled {
			if m.mock.RequireAuthHeader && !core.HasAuthHeader(ctx) {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
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
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		tokenString, err := extractBearerLikeToken(authHeader)
		if err != nil {
			m.logger.Debug("failed to parse authorization header", "error", err)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 1. Discover Fence API endpoint from token 'iss' claim
		apiEndpoint, _, err := m.parseToken(tokenString)
		if err != nil {
			m.logger.Debug("failed to parse token", "error", err)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 2. Initialize data-client FenceClient
		cred := &conf.Credential{
			AccessToken: tokenString,
			APIEndpoint: apiEndpoint,
		}

		// We use a no-op gen3 logger for the request client to avoid unnecessary side effects in middleware
		gen3Logger := logs.NewGen3Logger(m.logger, "", "drs-server")
		reqClient := request.NewRequestInterface(gen3Logger, cred, nil)
		fenceClient := fence.NewFenceClient(reqClient, cred, m.logger)

		// 3. Fetch user info (privileges)
		// NOTE: We are NOT caching here to ensure we always have the latest permissions from Fence.
		// If performance becomes an issue, consider adding a short-lived cache (e.g., 30s-1m).
		privs, err := fenceClient.CheckPrivileges(r.Context())
		if err != nil {
			m.logger.Debug("failed to check privileges with fence", "error", err)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// 4. Map privileges to authorized resources + methods
		authorizedResources, privileges := m.extractPrivileges(privs)
		ctx = context.WithValue(ctx, core.UserAuthzKey, authorizedResources)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, privileges)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
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
