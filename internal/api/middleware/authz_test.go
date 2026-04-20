package middleware

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/authz"
	"github.com/gofiber/fiber/v3"
)

func TestLocalModeBasicAuthEnforced(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "local", "user", "pass")
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.SetBasicAuth("user", "pass")
	resp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
}

func TestGen3ModeSetsContextWithoutAuthHeader(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		if !authz.IsGen3Mode(c.Context()) {
			t.Fatalf("expected gen3 mode in context")
		}
		if authz.HasAuthHeader(c.Context()) {
			t.Fatalf("did not expect auth header presence")
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestGen3ModeMalformedBearerStillPassesToNext(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		if !authz.HasAuthHeader(c.Context()) {
			t.Fatalf("expected auth header presence to be true")
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer malformed.token")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestParseToken(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")

	t.Run("valid token extracts endpoint and exp", func(t *testing.T) {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
		payload := base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"https://fence.example.org/user","exp":123.5}`))
		token := header + "." + payload + "."

		endpoint, exp, err := m.parseToken(token)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if endpoint != "https://fence.example.org" {
			t.Fatalf("expected endpoint https://fence.example.org, got %q", endpoint)
		}
		if exp != 123.5 {
			t.Fatalf("expected exp 123.5, got %v", exp)
		}
	})

	t.Run("missing iss fails", func(t *testing.T) {
		header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
		payload := base64.RawURLEncoding.EncodeToString([]byte(`{"exp":42}`))
		token := header + "." + payload + "."
		_, _, err := m.parseToken(token)
		if err == nil {
			t.Fatalf("expected parse error for missing iss claim")
		}
	})

	t.Run("invalid token fails", func(t *testing.T) {
		_, _, err := m.parseToken("not-a-token")
		if err == nil {
			t.Fatalf("expected parse error")
		}
	})
}

func TestExtractPrivileges(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	privs := map[string]any{
		"/programs/a/projects/b": []any{
			map[string]any{"service": "drs", "method": "read"},
			map[string]any{"service": "indexd", "method": "create"},
			map[string]any{"service": "*", "method": "delete"},
			map[string]any{"service": "fence", "method": "superuser"}, // ignored service
			map[string]any{"service": "drs"},                          // missing method
			"bad-entry",
		},
		"/programs/a": "not-a-list",
	}

	resources, out := m.extractPrivileges(privs)
	if len(resources) != 2 {
		t.Fatalf("expected 2 resources, got %d", len(resources))
	}
	methods := out["/programs/a/projects/b"]
	if !methods["read"] || !methods["create"] || !methods["delete"] {
		t.Fatalf("expected read/create/delete methods from accepted services, got %v", methods)
	}
	if methods["superuser"] {
		t.Fatalf("did not expect superuser method from unsupported service")
	}
	if len(out["/programs/a"]) != 0 {
		t.Fatalf("expected empty method map for malformed privilege list")
	}
}

func TestExtractBearerLikeToken(t *testing.T) {
	t.Run("bearer token", func(t *testing.T) {
		token, err := extractBearerLikeToken("Bearer abc.def.ghi")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "abc.def.ghi" {
			t.Fatalf("unexpected token: %q", token)
		}
	})

	t.Run("basic token in password", func(t *testing.T) {
		encoded := base64.StdEncoding.EncodeToString([]byte("oauth2:abc.def.ghi"))
		token, err := extractBearerLikeToken(fmt.Sprintf("Basic %s", encoded))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if token != "abc.def.ghi" {
			t.Fatalf("unexpected token: %q", token)
		}
	})

	t.Run("unsupported scheme", func(t *testing.T) {
		_, err := extractBearerLikeToken("Digest abc")
		if err == nil {
			t.Fatalf("expected error for unsupported scheme")
		}
	})
}

func TestGen3MockAuthInjectsPrivileges(t *testing.T) {
	t.Setenv("DRS_AUTH_MOCK_ENABLED", "true")
	t.Setenv("DRS_AUTH_MOCK_RESOURCES", "/data_file,/programs/cbds/projects/end_to_end_test")
	t.Setenv("DRS_AUTH_MOCK_METHODS", "read,file_upload,create,update,delete")

	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		if !authz.IsGen3Mode(c.Context()) {
			t.Fatalf("expected gen3 mode")
		}
		if !authz.HasMethodAccess(c.Context(), "read", []string{"/data_file"}) {
			t.Fatalf("expected read on /data_file")
		}
		if !authz.HasMethodAccess(c.Context(), "create", []string{"/programs/cbds/projects/end_to_end_test"}) {
			t.Fatalf("expected create on project resource")
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestGen3MockAuthRequireHeader(t *testing.T) {
	t.Setenv("DRS_AUTH_MOCK_ENABLED", "true")
	t.Setenv("DRS_AUTH_MOCK_REQUIRE_AUTH_HEADER", "true")
	t.Setenv("DRS_AUTH_MOCK_RESOURCES", "/data_file")
	t.Setenv("DRS_AUTH_MOCK_METHODS", "read")

	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	app := fiber.New()
	app.Use(m.FiberMiddleware())
	app.Get("/", func(c fiber.Ctx) error {
		// Without header, mock privileges should not be injected.
		if authz.HasMethodAccess(c.Context(), "read", []string{"/data_file"}) {
			t.Fatalf("did not expect read access without auth header")
		}
		return c.SendStatus(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestAuthzCacheSetGetPositive(t *testing.T) {
	c := newAuthzCache(authCacheConfig{
		Enabled:     true,
		TTL:         2 * time.Second,
		NegativeTTL: 1 * time.Second,
		MaxEntries:  10,
	})
	resources := []string{"/data_file"}
	privs := map[string]map[string]bool{
		"/data_file": {"read": true, "create": true},
	}

	c.set("k1", resources, privs, false)
	gotRes, gotPrivs, negative, ok := c.get("k1")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if negative {
		t.Fatalf("expected positive entry")
	}
	if len(gotRes) != 1 || gotRes[0] != "/data_file" {
		t.Fatalf("unexpected resources: %+v", gotRes)
	}
	if !gotPrivs["/data_file"]["read"] || !gotPrivs["/data_file"]["create"] {
		t.Fatalf("unexpected privileges: %+v", gotPrivs)
	}
}

func TestAuthzCacheSetGetNegative(t *testing.T) {
	c := newAuthzCache(authCacheConfig{
		Enabled:     true,
		TTL:         2 * time.Second,
		NegativeTTL: 2 * time.Second,
		MaxEntries:  10,
	})
	c.set("k2", nil, nil, true)
	_, _, negative, ok := c.get("k2")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if !negative {
		t.Fatalf("expected negative entry")
	}
}

func TestAuthzCacheExpires(t *testing.T) {
	c := newAuthzCache(authCacheConfig{
		Enabled:     true,
		TTL:         20 * time.Millisecond,
		NegativeTTL: 20 * time.Millisecond,
		MaxEntries:  10,
	})
	c.set("k3", []string{"/x"}, map[string]map[string]bool{"/x": {"read": true}}, false)
	time.Sleep(35 * time.Millisecond)
	_, _, _, ok := c.get("k3")
	if ok {
		t.Fatalf("expected cache miss after expiry")
	}
}

func TestAuthzCacheDeepCopy(t *testing.T) {
	c := newAuthzCache(authCacheConfig{
		Enabled:     true,
		TTL:         2 * time.Second,
		NegativeTTL: 1 * time.Second,
		MaxEntries:  10,
	})
	resources := []string{"/a"}
	privs := map[string]map[string]bool{
		"/a": {"read": true},
	}
	c.set("k4", resources, privs, false)

	// Mutate originals after set; cache should keep prior values.
	resources[0] = "/mutated"
	privs["/a"]["read"] = false

	gotRes, gotPrivs, _, ok := c.get("k4")
	if !ok {
		t.Fatalf("expected cache hit")
	}
	if gotRes[0] != "/a" {
		t.Fatalf("expected cached resource '/a', got %q", gotRes[0])
	}
	if !gotPrivs["/a"]["read"] {
		t.Fatalf("expected cached read=true, got %+v", gotPrivs)
	}
}

func TestAuthzMiddlewareScenarios(t *testing.T) {
	cases := []struct {
		name       string
		mode       string
		basicUser  string
		basicPass  string
		env        map[string]string
		authHeader string
		wantStatus int
		assert     func(*testing.T, fiber.Ctx)
	}{
		{
			name:       "local basic auth missing",
			mode:       "local",
			basicUser:  "user",
			basicPass:  "pass",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "local basic auth valid",
			mode:       "local",
			basicUser:  "user",
			basicPass:  "pass",
			authHeader: "Basic " + base64.StdEncoding.EncodeToString([]byte("user:pass")),
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, c fiber.Ctx) {
				if authz.IsGen3Mode(c.Context()) {
					t.Fatalf("did not expect gen3 mode in local auth")
				}
				if authz.HasAuthHeader(c.Context()) {
					t.Fatalf("did not expect auth header presence in local auth")
				}
			},
		},
		{
			name:       "gen3 no auth header",
			mode:       "gen3",
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, c fiber.Ctx) {
				if !authz.IsGen3Mode(c.Context()) {
					t.Fatalf("expected gen3 mode")
				}
				if authz.HasAuthHeader(c.Context()) {
					t.Fatalf("did not expect auth header presence")
				}
			},
		},
		{
			name:       "gen3 malformed bearer",
			mode:       "gen3",
			authHeader: "Bearer malformed.token",
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, c fiber.Ctx) {
				if !authz.IsGen3Mode(c.Context()) {
					t.Fatalf("expected gen3 mode")
				}
				if !authz.HasAuthHeader(c.Context()) {
					t.Fatalf("expected auth header presence")
				}
			},
		},
		{
			name: "gen3 mock auth injects privileges",
			mode: "gen3",
			env: map[string]string{
				"DRS_AUTH_MOCK_ENABLED":   "true",
				"DRS_AUTH_MOCK_RESOURCES": "/data_file,/programs/cbds/projects/end_to_end_test",
				"DRS_AUTH_MOCK_METHODS":   "read,create",
			},
			wantStatus: http.StatusOK,
			assert: func(t *testing.T, c fiber.Ctx) {
				if !authz.HasMethodAccess(c.Context(), "read", []string{"/data_file"}) {
					t.Fatalf("expected read access on /data_file")
				}
				if !authz.HasMethodAccess(c.Context(), "create", []string{"/programs/cbds/projects/end_to_end_test"}) {
					t.Fatalf("expected create access on scoped resource")
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			m := NewAuthzMiddleware(slog.Default(), tc.mode, tc.basicUser, tc.basicPass)
			app := fiber.New()
			handlerCalled := false
			app.Use(m.FiberMiddleware())
			app.Get("/", func(c fiber.Ctx) error {
				handlerCalled = true
				if tc.assert != nil {
					tc.assert(t, c)
				}
				return c.SendStatus(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}

			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("test request failed: %v", err)
			}
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("expected status %d, got %d", tc.wantStatus, resp.StatusCode)
			}
			if tc.wantStatus == http.StatusOK && !handlerCalled {
				t.Fatalf("expected handler to be called")
			}
			if tc.wantStatus != http.StatusOK && handlerCalled {
				t.Fatalf("did not expect handler to be called")
			}
		})
	}
}

func TestAuthzCacheLifecycle(t *testing.T) {
	cases := []struct {
		name        string
		cfg         authCacheConfig
		key         string
		resources   []string
		privileges  map[string]map[string]bool
		negative    bool
		mutateAfter bool
		wait        time.Duration
		wantOK      bool
		wantNeg     bool
		wantRead    bool
	}{
		{
			name: "positive hit",
			cfg: authCacheConfig{
				Enabled:     true,
				TTL:         2 * time.Second,
				NegativeTTL: 1 * time.Second,
				MaxEntries:  10,
			},
			key:       "k1",
			resources: []string{"/data_file"},
			privileges: map[string]map[string]bool{
				"/data_file": {"read": true, "create": true},
			},
			wantOK:   true,
			wantRead: true,
		},
		{
			name: "negative hit",
			cfg: authCacheConfig{
				Enabled:     true,
				TTL:         2 * time.Second,
				NegativeTTL: 2 * time.Second,
				MaxEntries:  10,
			},
			key:      "k2",
			negative: true,
			wantOK:   true,
			wantNeg:  true,
		},
		{
			name: "expiry",
			cfg: authCacheConfig{
				Enabled:     true,
				TTL:         20 * time.Millisecond,
				NegativeTTL: 20 * time.Millisecond,
				MaxEntries:  10,
			},
			key:       "k3",
			resources: []string{"/x"},
			privileges: map[string]map[string]bool{
				"/x": {"read": true},
			},
			wait:   35 * time.Millisecond,
			wantOK: false,
		},
		{
			name: "deep copy",
			cfg: authCacheConfig{
				Enabled:     true,
				TTL:         2 * time.Second,
				NegativeTTL: 1 * time.Second,
				MaxEntries:  10,
			},
			key:       "k4",
			resources: []string{"/a"},
			privileges: map[string]map[string]bool{
				"/a": {"read": true},
			},
			mutateAfter: true,
			wantOK:      true,
			wantRead:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newAuthzCache(tc.cfg)
			c.set(tc.key, tc.resources, tc.privileges, tc.negative)

			if tc.mutateAfter {
				tc.resources[0] = "/mutated"
				tc.privileges["/a"]["read"] = false
			}
			if tc.wait > 0 {
				time.Sleep(tc.wait)
			}

			gotRes, gotPrivs, negative, ok := c.get(tc.key)
			if ok != tc.wantOK {
				t.Fatalf("expected ok=%v, got %v", tc.wantOK, ok)
			}
			if !ok {
				return
			}
			if negative != tc.wantNeg {
				t.Fatalf("expected negative=%v, got %v", tc.wantNeg, negative)
			}
			if tc.wantOK && tc.wantRead {
				if len(gotRes) == 0 || gotRes[0] == "" {
					t.Fatalf("expected cached resource")
				}
				if !gotPrivs[gotRes[0]]["read"] {
					t.Fatalf("expected cached read privilege, got %+v", gotPrivs)
				}
			}
			if tc.mutateAfter {
				if gotRes[0] != "/a" {
					t.Fatalf("expected cached resource '/a', got %q", gotRes[0])
				}
				if !gotPrivs["/a"]["read"] {
					t.Fatalf("expected cached read=true, got %+v", gotPrivs)
				}
			}
		})
	}
}
