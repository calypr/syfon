package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	conf "github.com/calypr/syfon/client/config"
	"github.com/calypr/syfon/client/request"
	internalauth "github.com/calypr/syfon/internal/auth"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/plugin"
	"github.com/gofiber/fiber/v3"
	"golang.org/x/sync/singleflight"
)

type authenticationPluginManagerInterface interface {
	Authenticate(ctx context.Context, in *plugin.AuthenticationInput) (*plugin.AuthenticationOutput, error)
}

var newBearerTokenRequestor = request.NewBearerTokenRequestor

type AuthzMiddleware struct {
	logger             *slog.Logger
	mode               string
	basicUser          string
	basicPass          string
	mock               mockAuthConfig
	cache              *authzCache
	sf                 singleflight.Group
	pluginManager      pluginManagerInterface               // interface for testability
	authnPluginManager authenticationPluginManagerInterface // authentication plugin (interface)
	localUsers         *localAuthzStore
	localUsersErr      error
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
	if m.mode == "local" {
		localCSV := strings.TrimSpace(os.Getenv("DRS_LOCAL_AUTHZ_CSV"))
		if localCSV != "" {
			users, err := loadLocalAuthzCSV(localCSV)
			if err != nil {
				m.localUsersErr = err
				logger.Error("failed to load local authz csv", "path", localCSV, "err", err)
			} else {
				m.localUsers = users
			}
		}
	}
	// Config loading maps auth.plugin_paths.authz to this environment variable.
	pluginPath := os.Getenv("SYFON_AUTHZ_PLUGIN_PATH")
	if pluginPath != "" {
		pm, err := NewPluginManager(pluginPath)
		if err == nil {
			m.pluginManager = pm
		}
	}
	// Authentication plugin
	authnPluginPath := os.Getenv("SYFON_AUTHN_PLUGIN_PATH")
	if authnPluginPath != "" {
		apm, err := NewAuthenticationPluginManager(authnPluginPath)
		if err == nil {
			m.authnPluginManager = apm
		}
	}
	// Built-in plugins fallback
	if m.authnPluginManager == nil {
		if m.mode == "local" {
			m.authnPluginManager = &LocalAuthPlugin{BasicUser: basicUser, BasicPass: basicPass, Users: m.localUsers}
		} else if m.mode == "gen3" && !m.mock.Enabled {
			m.authnPluginManager = &Gen3AuthPlugin{MockConfig: m.mock, Logger: logger}
		}
	}
	return m
}

// FiberMiddleware returns a fiber middleware that extracts the token and fetches user info.

func (m *AuthzMiddleware) FiberMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		ctx, authHeader, session := m.prepareRequestContext(c)
		if m.mode != "gen3" {
			return m.handleLocalAuth(c, ctx, authHeader, session)
		}
		return m.handleGen3Auth(c, ctx, authHeader, session)
	}
}

func (m *AuthzMiddleware) prepareRequestContext(c fiber.Ctx) (context.Context, string, *internalauth.Session) {
	authHeader := c.Get(fiber.HeaderAuthorization)
	session := internalauth.NewSession(m.mode)
	if m.mode == "gen3" {
		session.AuthHeaderPresent = strings.TrimSpace(authHeader) != ""
		session.AuthzEnforced = true
	}
	ctx := internalauth.WithSession(c.Context(), session)
	return ctx, authHeader, session
}

func (m *AuthzMiddleware) applySession(c fiber.Ctx, ctx context.Context, session *internalauth.Session) error {
	ctx = internalauth.WithSession(ctx, session)
	c.SetContext(ctx)
	return c.Next()
}

func (m *AuthzMiddleware) authorizeWithPlugin(ctx context.Context, session *internalauth.Session, action, resource string) error {
	if m.pluginManager == nil {
		return nil
	}
	authzInput := &plugin.AuthorizationInput{
		RequestID: common.GetRequestID(ctx),
		Subject:   session.Subject,
		Action:    action,
		Resource:  resource,
		Claims:    session.Claims,
		Metadata:  map[string]interface{}{},
	}
	authzOutput, err := m.pluginManager.Authorize(ctx, authzInput)
	if err != nil {
		return fiber.NewError(fiber.StatusUnauthorized)
	}
	if !authzOutput.Allow {
		return fiber.NewError(fiber.StatusForbidden)
	}
	return nil
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

type pluginManagerInterface interface {
	Authorize(ctx context.Context, in *plugin.AuthorizationInput) (*plugin.AuthorizationOutput, error)
}
