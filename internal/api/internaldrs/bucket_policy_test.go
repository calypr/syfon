package internaldrs

import (
	"context"
	"testing"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/models"
)

func TestBucketPolicyHelpers(t *testing.T) {
	scope := models.BucketScope{
		Organization: "org",
		ProjectID:    "proj",
		Bucket:       "bucket-a",
	}
	resource, _ := sycommon.ResourcePath("org", "proj")

	t.Run("global bucket control access", func(t *testing.T) {
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
			"/services/internal/buckets": {"read": true, "create": true},
		})

		if !bucketControlAllowed(ctx, "read") {
			t.Fatal("expected global bucket control access")
		}
		if !bucketControlOpenAccess(context.Background(), "read") {
			t.Fatal("expected open access outside gen3 mode")
		}
		emptyCtx := policyTestContext("gen3", true, nil)
		if bucketControlAllowed(emptyCtx, "read") {
			t.Fatal("expected no global bucket control access without privileges")
		}
	})

	t.Run("scoped bucket access", func(t *testing.T) {
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
			resource: {"delete": true, "update": true},
		})

		if !bucketScopeAllowed(ctx, scope, "delete") {
			t.Fatal("expected scoped bucket access")
		}
		if !resourceAllowed(ctx, resource, "delete") {
			t.Fatal("expected resource access")
		}
	})

	t.Run("allowed bucket filtering by scope name", func(t *testing.T) {
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
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
