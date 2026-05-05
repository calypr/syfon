package common

import (
	"fmt"
	"strings"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/google/uuid"
)

var drsObjectIDNamespace = uuid.NewMD5(uuid.NameSpaceURL, []byte("calypr.org"))

func normalizeSHA256Checksum(raw string) string {
	v := strings.TrimSpace(strings.ToLower(raw))
	v = strings.TrimPrefix(v, "sha256:")
	return v
}

func canonicalProjectScope(authz []string) (string, error) {
	normalized := syfoncommon.NormalizeAccessResources(authz)
	if len(normalized) == 0 {
		return "", fmt.Errorf("%w: project scope is required when object id is not provided", ErrInvalidInput)
	}
	projectScopes := make([]string, 0, len(normalized))
	for _, resource := range normalized {
		org, project, ok := syfoncommon.ResourceScope(resource)
		if !ok || strings.TrimSpace(org) == "" || strings.TrimSpace(project) == "" {
			continue
		}
		projectScopes = append(projectScopes, resource)
	}
	if len(projectScopes) == 0 {
		return "", fmt.Errorf("%w: project scope is required when object id is not provided", ErrInvalidInput)
	}
	if len(projectScopes) > 1 {
		return "", fmt.Errorf("%w: exactly one project scope is required when object id is not provided", ErrInvalidInput)
	}
	return projectScopes[0], nil
}

// MintObjectIDFromChecksum returns a deterministic UUID for a checksum and a
// single canonical project scope. The generated UUID is stable across
// instances for the same sha256 and normalized project resource path.
func MintObjectIDFromChecksum(checksum string, authz []string) (string, error) {
	checksum = normalizeSHA256Checksum(checksum)
	if checksum == "" {
		return "", fmt.Errorf("%w: sha256 checksum is required when object id is not provided", ErrInvalidInput)
	}
	scope, err := canonicalProjectScope(authz)
	if err != nil {
		return "", err
	}
	seed := fmt.Sprintf("sha256:%s|%s", checksum, scope)
	return uuid.NewSHA1(drsObjectIDNamespace, []byte(seed)).String(), nil
}
