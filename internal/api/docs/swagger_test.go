package docs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"
)

func TestSwaggerUIRouteRootServed(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestSwaggerUIRouteTrailingSlash(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/swagger/", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "SwaggerUIBundle") {
		t.Fatalf("expected swagger html, got: %s", rr.Body.String())
	}
}

func TestOpenAPIRouteRootServed(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestOpenAPIRouteServed(t *testing.T) {
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
}

func TestAuxOpenAPIRoutesServed(t *testing.T) {
	router := mux.NewRouter()
	RegisterSwaggerRoutes(router)

	paths := []string{
		"/openapi-lfs.yaml",
		"/openapi-bucket.yaml",
		"/openapi-internal.yaml",
	}
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d body=%s", p, rr.Code, rr.Body.String())
		}
	}
}
