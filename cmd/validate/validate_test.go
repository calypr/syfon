package validate

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
)

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "openapi.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return p
}

func TestRegisterRoutes(t *testing.T) {
	app := fiber.New()
	registerHealthzRoute(app)
	registerServiceInfoRoute(app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected /healthz code: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Fatalf("unexpected /healthz body: %s", string(body))
	}

	os.Setenv("SERVICE_VERSION", "v1.2.3")
	defer os.Unsetenv("SERVICE_VERSION")
	resp, err = app.Test(httptest.NewRequest(http.MethodGet, "/service-info", nil))
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected /service-info code: %d", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"version":"v1.2.3"`) {
		t.Fatalf("unexpected /service-info body: %s", string(body))
	}
}

func TestSpecValidatorModesAndValidation(t *testing.T) {
	spec := `openapi: 3.0.3
info:
  title: test
  version: "1"
paths:
  /requires-header:
    get:
      parameters:
        - in: header
          name: X-Token
          required: true
          schema:
            type: string
      responses:
        "200":
          description: ok
`
	specPath := writeSpec(t, spec)

	validator, err := newSpecValidator(specPath, false)
	if err != nil {
		t.Fatalf("validator init failed: %v", err)
	}

	app := fiber.New()
	app.Use(validator)
	app.Get("/requires-header", func(c fiber.Ctx) error { return c.JSON(fiber.Map{"ok": true}) })

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/requires-header", nil))
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing required header, got %d", resp.StatusCode)
	}

	req := httptest.NewRequest(http.MethodGet, "/requires-header", nil)
	req.Header.Set("X-Token", "abc")
	resp, err = app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for valid request, got %d", resp.StatusCode)
	}

	lenient, err := newSpecValidator(specPath, true)
	if err != nil {
		t.Fatalf("lenient validator init failed: %v", err)
	}
	app2 := fiber.New()
	app2.Use(lenient)
	app2.Get("/not-in-spec", func(c fiber.Ctx) error { return c.SendStatus(http.StatusNoContent) })

	resp, err = app2.Test(httptest.NewRequest(http.MethodGet, "/not-in-spec", nil))
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected lenient pass-through for unknown route, got %d", resp.StatusCode)
	}
}

func TestDefaultSpecValidatorAndBadSpec(t *testing.T) {
	_, err := newDefaultSpecValidator("/definitely/missing/openapi.yaml")
	if err == nil {
		t.Fatal("expected missing spec error")
	}

	bad := writeSpec(t, "openapi: 3.0.3\ninfo:\n  title: bad\n")
	_, err = newSpecValidator(bad, false)
	if err == nil {
		t.Fatal("expected invalid spec error")
	}
}
