package middleware

import (
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/drs-server/db/core"
)

func TestLocalModeBasicAuthEnforced(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "local", "user", "pass")
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.SetBasicAuth("user", "pass")
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr2.Code)
	}
}

func TestGen3ModeSetsContextWithoutAuthHeader(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !core.IsGen3Mode(r.Context()) {
			t.Fatalf("expected gen3 mode in context")
		}
		if core.HasAuthHeader(r.Context()) {
			t.Fatalf("did not expect auth header presence")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestGen3ModeMalformedBearerStillPassesToNext(t *testing.T) {
	m := NewAuthzMiddleware(slog.Default(), "gen3", "", "")
	handler := m.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !core.HasAuthHeader(r.Context()) {
			t.Fatalf("expected auth header presence to be true")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer malformed.token")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
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
			map[string]any{"service": "fence", "method": "admin"}, // ignored service
			map[string]any{"service": "drs"},                      // missing method
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
	if methods["admin"] {
		t.Fatalf("did not expect admin method from unsupported service")
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
