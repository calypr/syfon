package copyproject

import (
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/calypr/syfon/apigen/bucketapi"
	"github.com/calypr/syfon/cmd/projectcopy"

	"github.com/calypr/syfon/internal/storage/address"
)

func defaultOrgScopePath(bucket, org string) string {
	return fmt.Sprintf("s3://%s/organizations/%s", bucket, org)
}

func defaultProjectScopePath(bucket, org, project string) string {
	return fmt.Sprintf("s3://%s/organizations/%s/projects/%s", bucket, org, project)
}

func remapOrgScopePath(srcScope *bucketapi.BucketScopeResponse, srcOrg, targetBucket, targetOrg string) (string, bool) {
	if srcScope == nil || srcScope.Path == nil || strings.TrimSpace(*srcScope.Path) == "" {
		return "", false
	}
	u, segs, ok := parseStorageURL(*srcScope.Path)
	if !ok {
		return "", false
	}
	if len(segs) > 0 && segs[len(segs)-1] == srcOrg {
		segs[len(segs)-1] = targetOrg
	} else {
		segs = append(segs, targetOrg)
	}
	u.Host = targetBucket
	u.Path = "/" + path.Join(segs...)
	return u.String(), true
}

func remapProjectScopePath(srcProject, srcOrg, dstOrg *bucketapi.BucketScopeResponse, srcScope, dstScope projectcopy.Scope, targetBucket string) (string, bool) {
	if dstOrg != nil && dstOrg.Path != nil && strings.TrimSpace(*dstOrg.Path) != "" && srcProject != nil && srcProject.Path != nil && strings.TrimSpace(*srcProject.Path) != "" {
		dstOrgURL, dstOrgSegs, okDst := parseStorageURL(*dstOrg.Path)
		srcProjURL, srcProjSegs, okProj := parseStorageURL(*srcProject.Path)
		srcOrgURL, srcOrgSegs, okOrg := parseStorageURL(pathOrEmpty(srcOrg))
		if okDst && okProj && okOrg && sameURLRoot(srcProjURL, srcOrgURL) && hasPathPrefix(srcProjSegs, srcOrgSegs) {
			relative := srcProjSegs[len(srcOrgSegs):]
			if len(relative) > 0 {
				dstOrgURL.Host = targetBucket
				dstOrgURL.Path = "/" + path.Join(append(dstOrgSegs, relative...)...)
				return dstOrgURL.String(), true
			}
		}
	}

	if srcProject == nil || srcProject.Path == nil || strings.TrimSpace(*srcProject.Path) == "" {
		return "", false
	}
	u, segs, ok := parseStorageURL(*srcProject.Path)
	if !ok {
		return "", false
	}
	switch {
	case len(segs) >= 2 && segs[len(segs)-2] == srcScope.Organization && segs[len(segs)-1] == srcScope.Project:
		segs[len(segs)-2] = dstScope.Organization
		segs[len(segs)-1] = dstScope.Project
	case len(segs) > 0 && segs[len(segs)-1] == srcScope.Project:
		segs[len(segs)-1] = dstScope.Project
	default:
		return "", false
	}
	u.Host = targetBucket
	u.Path = "/" + path.Join(segs...)
	return u.String(), true
}

func pathOrEmpty(scope *bucketapi.BucketScopeResponse) string {
	if scope == nil || scope.Path == nil {
		return ""
	}
	return strings.TrimSpace(*scope.Path)
}

func parseStorageURL(raw string) (*url.URL, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, false
	}
	u, err := url.Parse(raw)
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return nil, nil, false
	}
	if address.ProviderFromScheme(u.Scheme) == "" {
		return nil, nil, false
	}
	trimmed := strings.Trim(strings.TrimSpace(u.Path), "/")
	if trimmed == "" {
		return u, nil, true
	}
	return u, strings.Split(trimmed, "/"), true
}

func isBucketRootStoragePath(raw string) bool {
	_, segments, ok := parseStorageURL(raw)
	return ok && len(segments) == 0
}

func sameURLRoot(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(a.Scheme), strings.TrimSpace(b.Scheme)) && strings.EqualFold(strings.TrimSpace(a.Host), strings.TrimSpace(b.Host))
}

func hasPathPrefix(candidate, prefix []string) bool {
	if len(prefix) > len(candidate) {
		return false
	}
	for i := range prefix {
		if candidate[i] != prefix[i] {
			return false
		}
	}
	return true
}

func scopedObjectURL(projectPath, bucket, key, provider string) string {
	projectPath = strings.TrimSpace(projectPath)
	key = strings.Trim(strings.TrimSpace(key), "/")
	scheme := address.ProviderToScheme(provider)
	if projectPath == "" {
		return fmt.Sprintf("%s://%s/%s", scheme, bucket, key)
	}
	parsed, err := url.Parse(projectPath)
	if err != nil || strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Sprintf("%s://%s/%s", scheme, bucket, key)
	}
	parsed.Scheme = scheme
	if key != "" {
		parsed.Path = "/" + path.Join(strings.Trim(strings.TrimSpace(parsed.Path), "/"), key)
	}
	return parsed.String()
}

func sameServerURL(left, right string) bool {
	return strings.EqualFold(strings.TrimRight(strings.TrimSpace(left), "/"), strings.TrimRight(strings.TrimSpace(right), "/"))
}
