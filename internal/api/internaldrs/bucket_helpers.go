package internaldrs

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
)

const bucketControlResource = common.BucketControlResource

func readOptionalPath(path *string) string {
	if path == nil {
		return ""
	}
	return strings.TrimSpace(*path)
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err == nil {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func bucketControlAllowed(ctx context.Context, methods ...string) bool {
	return authz.HasGlobalBucketControlAccess(ctx, methods...)
}

func bucketControlOpenAccess(ctx context.Context, methods ...string) bool {
	return !authz.IsGen3Mode(ctx) || bucketControlAllowed(ctx, methods...)
}

func bucketScopeAllowed(ctx context.Context, scope models.BucketScope, methods ...string) bool {
	return authz.HasScopedBucketAccess(ctx, scope, methods...)
}

func resourceAllowed(ctx context.Context, resource string, methods ...string) bool {
	return authz.HasAnyMethodAccess(ctx, []string{resource}, methods...)
}

func allowedBucketsForScopes(ctx context.Context, scopes []models.BucketScope, methods ...string) map[string]bool {
	allowed := make(map[string]bool)
	for _, scope := range scopes {
		if bucketScopeAllowed(ctx, scope, methods...) {
			allowed[scope.Bucket] = true
		}
	}
	return allowed
}

func bucketsAllowedByNames(ctx context.Context, scopes []models.BucketScope, bucket string, methods ...string) bool {
	for _, scope := range scopes {
		if scope.Bucket != bucket {
			continue
		}
		if bucketScopeAllowed(ctx, scope, methods...) {
			return true
		}
	}
	return false
}

func authorizeBucketScopeWrite(ctx context.Context, organization, project string, methods ...string) error {
	if bucketControlAllowed(ctx, methods...) {
		return nil
	}
	if apimiddleware.MissingGen3AuthHeader(ctx) {
		return common.ErrUnauthorized
	}
	if strings.TrimSpace(organization) == "" {
		return common.ErrUnauthorized
	}
	res, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return err
	}
	if res == "" || !resourceAllowed(ctx, res, methods...) {
		return common.ErrUnauthorized
	}
	return nil
}

func authorizeBucketDelete(ctx context.Context, om *core.ObjectManager, bucket string) error {
	if bucketControlAllowed(ctx, "delete") {
		return nil
	}
	if apimiddleware.MissingGen3AuthHeader(ctx) {
		return common.ErrUnauthorized
	}
	scopes, err := om.ListBucketScopes(ctx)
	if err != nil {
		return err
	}
	if !bucketsAllowedByNames(ctx, scopes, bucket, "delete", "update") {
		return common.ErrUnauthorized
	}
	return nil
}
