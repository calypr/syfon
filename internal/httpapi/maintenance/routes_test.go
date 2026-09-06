package maintenance

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/maintenance/projectstorage"
	"github.com/calypr/syfon/internal/maintenance/scoperepair"
	"github.com/gofiber/fiber/v3"
)

func TestRegisterRoutesUsesDirectFiberCleanupParams(t *testing.T) {
	app := fiber.New()
	RegisterRoutes(app, Dependencies{})

	request := httptest.NewRequest(http.MethodDelete, "/data/projects/org/project", nil)
	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != "project storage service is not configured" {
		t.Fatalf("body = %q, want existing unsupported response", body)
	}
}

func TestInspectObjectRejectsMalformedURLWithExistingStatusAndBody(t *testing.T) {
	request := authenticatedRequest(http.MethodPost, RouteInspectObject, `{"object_url":"https://example.com/object"}`, nil)
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(request.Context())
		return c.Next()
	})
	RegisterRoutes(app, Dependencies{ProjectStorageService: projectstorage.NewService(projectstorage.Dependencies{})})

	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if string(body) != "object_url must be a valid s3://bucket/key URL" {
		t.Fatalf("body = %q, want invalid URL message", body)
	}
}

func TestInspectObjectUsesStrictJSONDecoding(t *testing.T) {
	request := authenticatedRequest(http.MethodPost, RouteInspectObject, `{"unexpected":true}`, nil)
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(request.Context())
		return c.Next()
	})
	RegisterRoutes(app, Dependencies{ProjectStorageService: projectstorage.NewService(projectstorage.Dependencies{})})

	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !strings.Contains(string(body), "Invalid request body: json: unknown field") {
		t.Fatalf("body = %q, want strict JSON error", body)
	}
}

func TestScopeRepairApplyChecksReadBeforeUpdate(t *testing.T) {
	privileges := map[string]map[string]bool{
		"/organization/org/project/project": {"read": true},
	}
	request := authenticatedRequest(http.MethodPost, RouteRepairScopeApply, `{"organization":"org","project":"project"}`, privileges)
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(request.Context())
		return c.Next()
	})
	RegisterRoutes(app, Dependencies{ScopeRepairService: scoperepair.NewService(nil, nil, nil, nil, nil)})

	response, err := app.Test(request)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for read-only caller", response.StatusCode)
	}
}

func authenticatedRequest(method, path, body string, privileges map[string]map[string]bool) *http.Request {
	session := access.NewSession("gen3")
	session.AuthHeaderPresent = true
	session.AuthzEnforced = true
	session.SetAuthorizations(nil, privileges, true)
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request.WithContext(access.WithSession(context.Background(), session))
}
