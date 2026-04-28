package internaldrs

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

func authStatusCode(ctx context.Context) int {
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func requireGen3AuthFiber(c fiber.Ctx) error {
	if authStatusCode(c.Context()) == http.StatusUnauthorized {
		return c.SendStatus(http.StatusUnauthorized)
	}
	return nil
}

func parseScopeQueryParts(organization, program, project string) (string, string, bool, error) {
	org := strings.TrimSpace(organization)
	if org == "" {
		org = strings.TrimSpace(program)
	}
	project = strings.TrimSpace(project)
	if project != "" && org == "" {
		return "", "", false, fmt.Errorf("organization is required when project is set")
	}
	if org != "" {
		return org, project, true, nil
	}
	return "", "", false, nil
}

func parseScopeQueryFiber(c fiber.Ctx) (string, string, bool, error) {
	return parseScopeQueryParts(c.Query("organization"), c.Query("program"), c.Query("project"))
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

func scopeResource(organization, project string) (string, error) {
	return sycommon.ResourcePath(organization, project)
}

func methodAllowedForAuthorizations(ctx context.Context, method string, authorizations map[string][]string) bool {
	return authz.HasMethodAccess(ctx, method, sycommon.AuthzMapToList(authorizations))
}

func requireMethodForObjectBatchAuthorizationsFiber(c fiber.Ctx, method string, objects []models.InternalObject) error {
	resources := objectBatchAuthorizationResources(objects)
	if !authz.HasMethodAccess(c.Context(), method, resources) {
		return c.Status(authStatusCode(c.Context())).SendString(
			fmt.Sprintf("unauthorized: %s access denied for object authorizations", method),
		)
	}
	return nil
}

func objectBatchAuthorizationResources(objects []models.InternalObject) []string {
	seen := make(map[string]struct{})
	for _, obj := range objects {
		for _, resource := range sycommon.AuthzMapToList(obj.Authorizations) {
			if resource == "" {
				continue
			}
			seen[resource] = struct{}{}
		}
	}
	resources := make([]string, 0, len(seen))
	for resource := range seen {
		resources = append(resources, resource)
	}
	return resources
}

func objectAuthzMatchesScope(obj models.InternalObject, org, project string) bool {
	if len(obj.Authorizations) == 0 {
		return false
	}
	projects, ok := obj.Authorizations[org]
	if !ok {
		return false
	}
	if len(projects) == 0 {
		return true
	}
	for _, p := range projects {
		if p == project {
			return true
		}
	}
	return false
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
