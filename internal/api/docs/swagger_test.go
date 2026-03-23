package docs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestSwaggerUIRoute(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "SwaggerUIBundle") {
		t.Fatalf("expected swagger html, got: %s", rr.Body.String())
	}
}

func TestOpenAPIRoute(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "openapi: 3.0.3") {
		t.Fatalf("expected openapi spec body, got: %s", rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "/info/lfs/objects/batch") {
		t.Fatalf("expected merged LFS route in openapi body")
	}
}

func TestOpenAPIRouteViaIndexPrefix(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/index/openapi.yaml", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "openapi: 3.0.3") {
		t.Fatalf("expected openapi spec body, got: %s", rr.Body.String())
	}
}
