package middleware

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
)

type mockPlugin struct {
	output *AuthorizationOutput
	err    error
}

func (m *mockPlugin) Authorize(ctx context.Context, in *AuthorizationInput) (*AuthorizationOutput, error) {
	return m.output, m.err
}

func addRecovery(app *fiber.App, t *testing.T) {
	app.Use(func(c fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("panic: %v", r)
			}
		}()
		return c.Next()
	})
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPluginIntegrationAllow(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: &AuthorizationOutput{
			Allow: true,
			Obligations: map[string]interface{}{
				"resources": []interface{}{ "/foo", "/bar" },
				"privileges": map[string]interface{}{
					"/foo": []interface{}{ "read", "write" },
					"/bar": []interface{}{ "read" },
				},
			},
		},
		err: nil,
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPluginIntegrationDeny(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: &AuthorizationOutput{Allow: false, Reason: "denied"},
		err: nil,
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 403 {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestPluginIntegrationError(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: nil,
		err: errors.New("plugin error"),
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestPluginIntegration_MissingClaims(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: &AuthorizationOutput{Allow: true, Obligations: map[string]interface{}{}},
		err: nil,
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer invalidtoken") // Not a real JWT
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPluginIntegration_PluginUnavailable(t *testing.T) {
	mw := &AuthzMiddleware{pluginManager: nil, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 401 {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
}

func TestPluginIntegration_UnexpectedObligations(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: &AuthorizationOutput{
			Allow: true,
			Obligations: map[string]interface{}{
				"resources": 123, // Not a []interface{}
				"privileges": "not a map",
			},
		},
		err: nil,
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	req.Header.Set("Authorization", "Bearer testtoken")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestPluginIntegration_NoAuthHeader(t *testing.T) {
	pm := &PluginManager{client: nil}
	pm.client = &PluginClient{raw: &mockPlugin{
		output: &AuthorizationOutput{Allow: true, Obligations: map[string]interface{}{}},
		err: nil,
	}}
	mw := &AuthzMiddleware{pluginManager: pm, mode: "gen3", logger: newTestLogger()}
	app := fiber.New()
	addRecovery(app, t)
	app.Use(mw.FiberMiddleware())
	app.Get("/foo", func(c fiber.Ctx) error {
		return c.SendStatus(200)
	})
	req := httptest.NewRequest(http.MethodGet, "/foo", nil)
	// No Authorization header
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}
