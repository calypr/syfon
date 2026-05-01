package metrics

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/authz"
)

func (s *MetricsServer) checkAuth(ctx context.Context) (metricsAccess, int, bool) {
	access, err := resolveMetricsAccess(ctx)
	if err != nil {
		return metricsAccess{}, http.StatusBadRequest, false
	}

	if !authz.IsAuthzEnforced(ctx) {
		return access, 0, true
	}
	if apimiddleware.MissingGen3AuthHeader(ctx) {
		return access, http.StatusUnauthorized, false
	}

	// Baseline read access for metrics: global access or scoped access
	if authz.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		authz.HasMethodAccess(ctx, "read", []string{"/programs"}) {
		return access, 0, true
	}

	if access.isScoped() {
		scope, err := sycommon.ResourcePath(access.organization, access.project)
		if err != nil {
			return access, http.StatusBadRequest, false
		}
		if authz.HasMethodAccess(ctx, "read", []string{scope}) {
			return access, 0, true
		}
		return access, http.StatusForbidden, false
	}

	scopes := readableMetricsScopes(ctx)
	if len(scopes) > 0 {
		access.scopes = scopes
		return access, 0, true
	}

	return access, http.StatusForbidden, false
}

type metricsAccess struct {
	organization string
	project      string
	scopes       []metricsScope
}

func (a metricsAccess) isScoped() bool {
	return strings.TrimSpace(a.organization) != ""
}

func (a metricsAccess) hasScopeAggregate() bool {
	return !a.isScoped() && len(a.scopes) > 0
}

type metricsScope struct {
	organization string
	project      string
}

func resolveMetricsAccess(ctx context.Context) (metricsAccess, error) {
	org, project, _, err := parseScopeQuery(ctx)
	if err != nil {
		return metricsAccess{}, err
	}
	return metricsAccess{organization: org, project: project}, nil
}

func readableMetricsScopes(ctx context.Context) []metricsScope {
	privs := authz.GetUserPrivileges(ctx)
	scopes := make([]metricsScope, 0, len(privs))
	seen := map[string]bool{}
	for resource, methods := range privs {
		if !(methods["read"] || methods["*"]) {
			continue
		}
		scope, ok := metricsScopeFromResource(resource)
		if !ok {
			continue
		}
		key := scope.organization + "\x00" + scope.project
		if seen[key] {
			continue
		}
		seen[key] = true
		scopes = append(scopes, scope)
	}
	orgWide := map[string]bool{}
	for _, scope := range scopes {
		if scope.project == "" {
			orgWide[scope.organization] = true
		}
	}
	if len(orgWide) > 0 {
		filtered := scopes[:0]
		for _, scope := range scopes {
			if scope.project != "" && orgWide[scope.organization] {
				continue
			}
			filtered = append(filtered, scope)
		}
		scopes = filtered
	}
	sort.Slice(scopes, func(i, j int) bool {
		if scopes[i].organization == scopes[j].organization {
			return scopes[i].project < scopes[j].project
		}
		return scopes[i].organization < scopes[j].organization
	})
	return scopes
}

func metricsScopeFromResource(resource string) (metricsScope, bool) {
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	if len(parts) == 2 && parts[0] == "programs" && parts[1] != "" {
		return metricsScope{organization: parts[1]}, true
	}
	if len(parts) == 4 && parts[0] == "programs" && parts[2] == "projects" && parts[1] != "" && parts[3] != "" {
		return metricsScope{organization: parts[1], project: parts[3]}, true
	}
	return metricsScope{}, false
}

func parseScopeQuery(ctx context.Context) (string, string, bool, error) {
	params, _ := ctx.Value(metricsQueryContextKey{}).(metricsQueryParams)
	org := strings.TrimSpace(params.organization)
	if org == "" {
		org = strings.TrimSpace(params.program)
	}
	project := strings.TrimSpace(params.project)
	if project != "" && org == "" {
		return "", "", false, errors.New("organization is required when project is set")
	}
	if org != "" {
		return org, project, true, nil
	}
	return "", "", false, nil
}
