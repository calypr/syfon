package common

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

var drsObjectIDNamespace = uuid.NewMD5(uuid.NameSpaceURL, []byte("calypr.org"))

func normalizeSHA256Checksum(raw string) string {
	v := strings.TrimSpace(strings.ToLower(raw))
	v = strings.TrimPrefix(v, "sha256:")
	return v
}

func projectKeyFromAuthz(authz []string) string {
	if len(authz) == 0 {
		return ""
	}
	scope := ParseResourcePath(authz[0])
	if scope.Organization == "" || scope.Project == "" {
		return ""
	}
	return fmt.Sprintf("%s/%s", scope.Organization, scope.Project)
}

// MintObjectIDFromChecksum returns a deterministic UUID for a checksum.
// The generated UUID is scoped to the first lexical "org/project" authorization
// when available, which avoids cross-project collisions for identical content.
func MintObjectIDFromChecksum(checksum string, authz []string) string {
	checksum = normalizeSHA256Checksum(checksum)
	if checksum == "" {
		return uuid.NewString()
	}
	seed := checksum
	if projectKey := projectKeyFromAuthz(authz); projectKey != "" {
		seed = projectKey + ":" + checksum
	}
	return uuid.NewSHA1(drsObjectIDNamespace, []byte(seed)).String()
}
