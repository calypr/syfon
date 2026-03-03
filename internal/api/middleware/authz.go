package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/data-client/conf"
	"github.com/calypr/data-client/fence"
	"github.com/calypr/data-client/logs"
	"github.com/calypr/data-client/request"
	"github.com/calypr/drs-server/db/core"
	"github.com/golang-jwt/jwt/v5"
)

type AuthzMiddleware struct {
	logger *slog.Logger
}

func NewAuthzMiddleware(logger *slog.Logger) *AuthzMiddleware {
	return &AuthzMiddleware{
		logger: logger,
	}
}

// Middleware returns a mux middleware that extracts the token and fetches user info.
func (m *AuthzMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			next.ServeHTTP(w, r)
			return
		}

		tokenString := strings.TrimPrefix(authHeader, "Bearer ")
		tokenString = strings.TrimSpace(tokenString)

		// 1. Discover Fence API endpoint from token 'iss' claim
		apiEndpoint, _, err := m.parseToken(tokenString)
		if err != nil {
			m.logger.Debug("failed to parse token", "error", err)
			next.ServeHTTP(w, r)
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
			next.ServeHTTP(w, r)
			return
		}

		// 4. Map privileges to authorized resources
		authorizedResources := m.extractResources(privs)
		ctx := context.WithValue(r.Context(), core.UserAuthzKey, authorizedResources)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
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

func (m *AuthzMiddleware) extractResources(privs map[string]any) []string {
	resources := make([]string, 0, len(privs))
	for path := range privs {
		resources = append(resources, path)
	}
	return resources
}
