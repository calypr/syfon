package validate

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func writeSpec(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "openapi.yaml")
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatalf("write spec: %v", err)
	}
	return p
}

func TestRegisterRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	registerHealthzRoute(r)
	registerServiceInfoRoute(r)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"ok"`) {
		t.Fatalf("unexpected /healthz response: code=%d body=%s", w.Code, w.Body.String())
	}

	os.Setenv("SERVICE_VERSION", "v1.2.3")
	defer os.Unsetenv("SERVICE_VERSION")
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/service-info", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"version":"v1.2.3"`) {
		t.Fatalf("unexpected /service-info response: code=%d body=%s", w.Code, w.Body.String())
	}
}

func TestSpecValidatorModesAndValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
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

	r := gin.New()
	r.Use(validator)
	r.GET("/requires-header", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"ok": true}) })

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/requires-header", nil)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing required header, got %d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/requires-header", nil)
	req.Header.Set("X-Token", "abc")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 for valid request, got %d body=%s", w.Code, w.Body.String())
	}

	lenient, err := newSpecValidator(specPath, true)
	if err != nil {
		t.Fatalf("lenient validator init failed: %v", err)
	}
	r2 := gin.New()
	r2.Use(lenient)
	r2.GET("/not-in-spec", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/not-in-spec", nil)
	r2.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("expected lenient pass-through for unknown route, got %d", w.Code)
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
