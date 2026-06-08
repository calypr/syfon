package internaldrs

import (
	"context"
	"errors"
	"testing"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
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

	t.Run("bucket scope write allows org descendant creators", func(t *testing.T) {
		orgResource, _ := sycommon.ResourcePath("org", "")
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
			orgResource: {"arborist:create-descendant": true},
		})

		if err := authorizeBucketScopeWrite(ctx, "org", "new-project", "create", "update"); err != nil {
			t.Fatalf("expected org descendant creator to be allowed, got %v", err)
		}
	})

	t.Run("bucket scope write does not allow top-level program creator without org or project ownership", func(t *testing.T) {
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
			"/programs": {"arborist:create-descendant": true},
		})

		if err := authorizeBucketScopeWrite(ctx, "brand_new_org", "new-project", "create", "update"); err == nil {
			t.Fatal("expected top-level program creator alone to be denied")
		}
	})

	t.Run("bucket scope write does not treat requestor create as arborist create-descendant", func(t *testing.T) {
		orgResource, _ := sycommon.ResourcePath("org", "")
		ctx := policyTestContext("gen3", true, map[string]map[string]bool{
			orgResource: {"requestor:create": true},
		})

		err := authorizeBucketScopeWrite(ctx, "org", "new-project", "create", "update")
		if err == nil {
			t.Fatal("expected requestor create alone to be denied")
		}
		if !errors.Is(err, common.ErrUnauthorized) {
			t.Fatalf("expected unauthorized error, got %v", err)
		}
	})
}
