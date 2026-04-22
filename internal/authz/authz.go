package authz

import (
	"context"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

// GetUserAuthz returns the list of resources the user is authorized to access.
// If not found, returns empty list (no access to protected resources).
func GetUserAuthz(ctx context.Context) []string {
	val := ctx.Value(common.UserAuthzKey)
	if val == nil {
		return []string{}
	}
	if list, ok := val.([]string); ok {
		return list
	}
	return []string{}
}

// CheckAccess verifies if a user has access to a record based on RBAC resources.
// A record is accessible if:
// 1. It has NO required resources (public).
// 2. OR the user has at least one of the resources listed on the record.
func CheckAccess(recordResources []string, userResources []string) bool {
	if len(recordResources) == 0 {
		return true // Public
	}
	// Create map for O(1) check
	userMap := make(map[string]bool)
	for _, r := range userResources {
		userMap[r] = true
	}

	for _, r := range recordResources {
		if userMap[r] {
			return true
		}
	}
	return false
}

func HasAuthHeader(ctx context.Context) bool {
	v := ctx.Value(common.AuthHeaderPresentKey)
	ok, _ := v.(bool)
	return ok
}

func IsGen3Mode(ctx context.Context) bool {
	v := ctx.Value(common.AuthModeKey)
	mode, _ := v.(string)
	return mode == "gen3"
}

func GetUserPrivileges(ctx context.Context) map[string]map[string]bool {
	v := ctx.Value(common.UserPrivilegesKey)
	if v == nil {
		return map[string]map[string]bool{}
	}
	if p, ok := v.(map[string]map[string]bool); ok {
		return p
	}
	return map[string]map[string]bool{}
}

func HasMethodAccess(ctx context.Context, method string, resources []string) bool {
	if !IsGen3Mode(ctx) {
		return true
	}
	if !HasAuthHeader(ctx) {
		return false
	}
	privs := GetUserPrivileges(ctx)
	if len(resources) == 0 {
		return false
	}
	for _, resource := range resources {
		methods, ok := privs[resource]
		if !ok {
			return false
		}
		if methods[method] || methods["*"] {
			continue
		}
		return false
	}
	return true
}

func HasAnyMethodAccess(ctx context.Context, resources []string, methods ...string) bool {
	if !IsGen3Mode(ctx) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	for _, m := range methods {
		if HasMethodAccess(ctx, m, resources) {
			return true
		}
	}
	return false
}

func AuthStatusCode(ctx context.Context) int {
	if IsGen3Mode(ctx) && !HasAuthHeader(ctx) {
		return 401
	}
	return 403
}

// HasGlobalBucketControlAccess checks if the user has overarching control over bucket registration.
func HasGlobalBucketControlAccess(ctx context.Context, methods ...string) bool {
	return HasAnyMethodAccess(ctx, []string{common.BucketControlResource}, methods...)
}

// HasScopedBucketAccess checks if a user has access to a specific bucket based on a project/org scope.
func HasScopedBucketAccess(ctx context.Context, scope models.BucketScope, methods ...string) bool {
	resource, err := sycommon.ResourcePath(scope.Organization, scope.ProjectID)
	if err != nil || resource == "" {
		return false
	}
	return HasAnyMethodAccess(ctx, []string{resource}, methods...)
}
