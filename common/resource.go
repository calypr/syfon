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
