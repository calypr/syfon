package internaldrs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

func TestParseScopeQueryParts(t *testing.T) {
	tests := []struct {
		name         string
		authz        string
		organization string
		program      string
		project      string
		want         string
		wantOK       bool
		wantErr      bool
	}{
		{
			name:         "authz takes precedence",
			authz:        "/programs/explicit/projects/path",
			organization: "ignored",
			program:      "ignored",
			project:      "ignored",
			want:         "/programs/explicit/projects/path",
			wantOK:       true,
		},
		{
			name:         "organization and project build a resource path",
			organization: "org",
			project:      "proj",
			want:         "/programs/org/projects/proj",
			wantOK:       true,
		},
		{
			name:    "program falls back when organization is empty",
			program: "org",
			want:    "/programs/org",
			wantOK:  true,
		},
		{
			name:    "project without organization is invalid",
			project: "proj",
			wantErr: true,
		},
		{
			name: "empty scope is allowed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok, err := parseScopeQueryParts(tt.authz, tt.organization, tt.program, tt.project)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if ok {
					t.Fatal("expected ok=false on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.wantOK {
				t.Fatalf("unexpected ok: got %v want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Fatalf("unexpected scope: got %q want %q", got, tt.want)
			}
		})
	}
}

func TestRequireGen3AuthFiber(t *testing.T) {
	t.Run("missing auth header returns unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req = req.WithContext(context.WithValue(req.Context(), common.AuthModeKey, "gen3"))
		rr := httptest.NewRecorder()

		serveFiberHandlerHTTP(rr, req, "/", func(c fiber.Ctx) error {
			return requireGen3AuthFiber(c)
		})

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rr.Code)
		}
	})

	t.Run("present auth header passes through", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()

		serveFiberHandlerHTTP(rr, req, "/", func(c fiber.Ctx) error {
			return requireGen3AuthFiber(c)
		})

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rr.Code)
		}
	})
}

func TestBucketPolicyHelpers(t *testing.T) {
	scope := models.BucketScope{
		Organization: "org",
		ProjectID:    "proj",
		Bucket:       "bucket-a",
	}
	resource := common.ResourcePathForScope("org", "proj")

	t.Run("global bucket control access", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			common.BucketControlResource: {"read": true, "create": true},
		})

		if !bucketControlAllowed(ctx, "read") {
			t.Fatal("expected global bucket control access")
		}
		if !bucketControlOpenAccess(context.Background(), "read") {
			t.Fatal("expected open access outside gen3 mode")
		}
		emptyCtx := context.WithValue(context.Background(), common.AuthModeKey, "gen3")
		emptyCtx = context.WithValue(emptyCtx, common.AuthHeaderPresentKey, true)
		if bucketControlAllowed(emptyCtx, "read") {
			t.Fatal("expected no global bucket control access without privileges")
		}
	})

	t.Run("scoped bucket access", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			resource: {"delete": true, "update": true},
		})

		if !bucketScopeAllowed(ctx, scope, "delete") {
			t.Fatal("expected scoped bucket access")
		}
		if !resourceAllowed(ctx, resource, "delete") {
			t.Fatal("expected resource access")
		}
		if !methodAllowedForAuthorizations(ctx, "delete", []string{resource}) {
			t.Fatal("expected authorization list access")
		}
	})

	t.Run("allowed bucket filtering by scope name", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			resource: {"read": true},
		})

		allowed := allowedBucketsForScopes(ctx, []models.BucketScope{scope}, "read")
		if !allowed["bucket-a"] {
			t.Fatal("expected bucket to be allowed")
		}
		if !bucketsAllowedByNames(ctx, []models.BucketScope{scope}, "bucket-a", "read") {
			t.Fatal("expected bucket name match to be allowed")
		}
		if bucketsAllowedByNames(ctx, []models.BucketScope{scope}, "bucket-b", "read") {
			t.Fatal("expected non-matching bucket to be denied")
		}
	})
}
