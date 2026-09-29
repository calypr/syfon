package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/httpapi/apidocs"
	"github.com/gofiber/fiber/v3"
)

func TestDocsRoutesBypassAuthorizationButProtectedRoutesDoNot(t *testing.T) {
	app := fiber.New()
	RegisterRoutes(app, Dependencies{
		RequestIDs: RequestIDHandler(nil),
		Authorization: AuthorizationHandler(AuthzOptions{
			Mode:      "local",
			Evaluator: &fixedEvaluator{decision: access.DecisionUnauthorized},
		}),
	}, Options{Docs: true, GA4GH: true})

	for _, path := range []string{
		apidocs.RouteSwaggerUI,
		apidocs.RouteSwaggerUIAlt,
		apidocs.RouteOpenAPISpec,
		apidocs.RouteLFSSpec,
		apidocs.RouteBucketSpec,
		apidocs.RouteInternalSpec,
		apidocs.RouteErrorSpec,
		apidocs.RouteSwaggerUI + "/swagger-ui.css",
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set(requestIDHeader, "docs-request-id")
		resp, err := app.Test(request)
		if err != nil {
			t.Fatalf("GET %s failed: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d", path, resp.StatusCode, http.StatusOK)
		}
		if got := resp.Header.Get(requestIDHeader); got != "docs-request-id" {
			t.Fatalf("GET %s request ID = %q, want docs-request-id", path, got)
		}
	}

	request := httptest.NewRequest(http.MethodPost, "/ga4gh/drs/v1/objects/register", nil)
	request.Header.Set(requestIDHeader, "api-request-id")
	resp, err := app.Test(request)
	if err != nil {
		t.Fatalf("protected request failed: %v", err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("protected route status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
	if got := resp.Header.Get(requestIDHeader); got != "api-request-id" {
		t.Fatalf("protected route request ID = %q, want api-request-id", got)
	}
}
