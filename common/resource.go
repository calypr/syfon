package common

import (
	"fmt"
	"strings"
)

// ResourcePath returns the GA4GH resource path for an org/project scope,
// e.g. "/programs/myorg/projects/myproject".
func ResourcePath(org, project string) (string, error) {
	org = strings.TrimSpace(org)
	project = strings.TrimSpace(project)
	if org == "" && project == "" {
		return "", nil
	}
	if org == "" {
		return "", fmt.Errorf("organization required when project is specified")
	}
	if project == "" {
		return "/programs/" + org, nil
	}
	return "/programs/" + org + "/projects/" + project, nil
}

// StoragePrefix returns the storage path prefix for an org/project scope
// (no leading slash), e.g. "programs/myorg/projects/myproject".
func StoragePrefix(org, project string) string {
	org = strings.TrimSpace(org)
	project = strings.TrimSpace(project)
	if org == "" {
		return ""
	}
	if project == "" {
		return "programs/" + org
	}
	return "programs/" + org + "/projects/" + project
}

// NormalizeChecksum trims whitespace and an optional sha256: prefix.
func NormalizeChecksum(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "sha256:")
	return strings.TrimSpace(raw)
}

// AuthzMapFromScope builds the wire-format authorizations map from an org and project.
// Empty project means org-wide access (value is empty slice).
func AuthzMapFromScope(org, project string) map[string][]string {
	org = strings.TrimSpace(org)
	if org == "" {
		return nil
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return map[string][]string{org: {}}
	}
	return map[string][]string{org: {project}}
}

// AuthzListToMap converts a list of GA4GH resource-path strings (e.g.
// "/programs/org/projects/proj") to the wire-format org→projects map.
func AuthzListToMap(paths []string) map[string][]string {
	if len(paths) == 0 {
		return nil
	}
	result := make(map[string][]string)
	for _, path := range paths {
		path = strings.TrimSpace(path)
		org, project := parseResourcePath(path)
		if org == "" {
			continue
		}
		if _, ok := result[org]; !ok {
			result[org] = []string{}
		}
		if project != "" {
			result[org] = append(result[org], project)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// AuthzMapToList converts the wire-format org→projects map back to a list of
// GA4GH resource-path strings for internal storage.
func AuthzMapToList(authzMap map[string][]string) []string {
	if len(authzMap) == 0 {
		return nil
	}
	out := make([]string, 0, len(authzMap))
	for org, projects := range authzMap {
		if len(projects) == 0 {
			out = append(out, "/programs/"+org)
		} else {
			for _, project := range projects {
				out = append(out, "/programs/"+org+"/projects/"+project)
			}
		}
	}
	return out
}

// parseResourcePath splits "/programs/org" or "/programs/org/projects/proj" into (org, project).
func parseResourcePath(path string) (string, string) {
	path = strings.TrimPrefix(path, "/programs/")
	parts := strings.SplitN(path, "/projects/", 2)
	if len(parts) == 1 {
		return strings.TrimSpace(parts[0]), ""
	}
	return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
}
