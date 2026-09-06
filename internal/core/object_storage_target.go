package core

import (
	"context"
	"fmt"
	"path"
	"strings"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/storage/address"
)

type scopedStorageTarget struct {
	Bucket string
	Key    string
	URL    string
}

func (m *ObjectManager) resolveScopedUploadTarget(ctx context.Context, organization, project, key string) (scopedStorageTarget, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	key = strings.Trim(strings.TrimSpace(key), "/")
	if organization == "" {
		return scopedStorageTarget{}, fmt.Errorf("%w: organization is required", faults.ErrInvalidInput)
	}
	if _, err := syfoncommon.ResourcePath(organization, project); err != nil {
		return scopedStorageTarget{}, fmt.Errorf("%w: %v", faults.ErrInvalidInput, err)
	}

	scopes := make([]buckets.Scope, 0, 2)
	if scope, found, err := m.bucketService.LookupBucketScope(ctx, organization, ""); err != nil {
		return scopedStorageTarget{}, err
	} else if found {
		scopes = append(scopes, scope)
	}
	if project != "" {
		if scope, found, err := m.bucketService.LookupBucketScope(ctx, organization, project); err != nil {
			return scopedStorageTarget{}, err
		} else if found {
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		if project != "" {
			return scopedStorageTarget{}, fmt.Errorf("%w: no bucket scope configured for organization %q project %q", faults.ErrInvalidInput, organization, project)
		}
		return scopedStorageTarget{}, fmt.Errorf("%w: no bucket scope configured for organization %q", faults.ErrInvalidInput, organization)
	}

	bucket := ""
	for _, scope := range scopes {
		if value := strings.TrimSpace(scope.Bucket); value != "" {
			bucket = value
		}
	}
	if bucket == "" {
		return scopedStorageTarget{}, fmt.Errorf("%w: unable to resolve scoped storage bucket for organization %q project %q", faults.ErrInvalidInput, organization, project)
	}
	key = normalizeScopedStorageKey(key, scopes)
	if key == "" {
		return scopedStorageTarget{}, fmt.Errorf("%w: unable to resolve scoped storage key for organization %q project %q", faults.ErrInvalidInput, organization, project)
	}
	return scopedStorageTarget{Bucket: bucket, Key: key, URL: address.BucketToURL(bucket, key)}, nil
}

func normalizeScopedStorageKey(key string, scopes []buckets.Scope) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefixes := normalizedScopePrefixes(scopes)
	for _, prefix := range prefixes {
		key = trimLeadingStoragePrefix(key, prefix)
	}
	prefix := strings.Join(prefixes, "/")
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return path.Join(prefix, key)
}

func normalizedScopePrefixes(scopes []buckets.Scope) []string {
	prefixes := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
		if prefix == "" {
			continue
		}
		if len(prefixes) == 0 {
			prefixes = append(prefixes, prefix)
			continue
		}
		last := prefixes[len(prefixes)-1]
		switch {
		case prefix == last:
		case strings.HasPrefix(prefix, last+"/"):
			prefixes[len(prefixes)-1] = prefix
		case strings.HasPrefix(last, prefix+"/"):
		default:
			prefixes = append(prefixes, prefix)
		}
	}
	return prefixes
}

func trimLeadingStoragePrefix(key, prefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if key == prefix {
		return ""
	}
	return strings.TrimPrefix(key, prefix+"/")
}
