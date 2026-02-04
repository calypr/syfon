package core

import (
	"context"
)

type AuthzContextKey string

const (
	// UserAuthzKey is the context key for the user's authorized resources list
	UserAuthzKey AuthzContextKey = "user_authz"
)

// GetUserAuthz returns the list of resources the user is authorized to access.
// If not found, returns empty list (no access to protected resources).
func GetUserAuthz(ctx context.Context) []string {
	val := ctx.Value(UserAuthzKey)
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
