package core

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
)

// SignURL generates a signed URL for an object's access method.
func (m *ObjectManager) SignURL(ctx context.Context, accessURL string, options urlmanager.SignOptions) (string, error) {
	return m.uM.SignURL(ctx, m.resolveSigningBucket(ctx, accessURL), accessURL, options)
}

func (m *ObjectManager) SignObjectURL(ctx context.Context, obj *models.InternalObject, accessURL string, options urlmanager.SignOptions) (string, error) {
	scopedURL, err := m.resolveScopedStorageURL(ctx, obj, accessURL)
	if err != nil {
		return "", err
	}
	return m.SignURL(ctx, scopedURL, options)
}

func (m *ObjectManager) ResolveObjectScopedStorageURL(ctx context.Context, obj *models.InternalObject, accessURL string) (string, error) {
	return m.resolveScopedStorageURL(ctx, obj, accessURL)
}

func (m *ObjectManager) ResolveCanonicalObjectUploadURL(ctx context.Context, obj *models.InternalObject, bucketName string) (string, error) {
	if obj == nil {
		return "", fmt.Errorf("object is required")
	}

	scopes, err := m.bucketScopesForObject(ctx, obj)
	if err != nil {
		return "", err
	}
	if len(scopes) > 0 {
		targetBucket := strings.TrimSpace(bucketName)
		for _, scope := range scopes {
			if strings.TrimSpace(scope.Bucket) != "" {
				targetBucket = strings.TrimSpace(scope.Bucket)
			}
		}
		if targetBucket == "" {
			return "", fmt.Errorf("unable to resolve scoped upload bucket for object %s", obj.Id)
		}
		targetKey := ""
		if checksum, ok := common.CanonicalSHA256(obj.Checksums); ok && strings.TrimSpace(checksum) != "" {
			targetKey = normalizeScopedStorageKey(checksum, scopes)
		} else if bucket, key, ok := parseS3Location(FirstSupportedAccessURL(obj)); ok {
			_ = bucket
			targetKey = normalizeScopedStorageKey(key, scopes)
		}
		if strings.TrimSpace(targetKey) == "" {
			return "", fmt.Errorf("unable to resolve scoped upload key for object %s", obj.Id)
		}
		return common.S3Prefix + targetBucket + "/" + targetKey, nil
	}

	if strings.TrimSpace(bucketName) != "" {
		bucket, err := m.ResolveBucket(ctx, bucketName)
		if err != nil {
			return "", err
		}
		if key, ok := m.ResolveObjectRemotePath(ctx, obj.Id, bucket); ok && strings.TrimSpace(key) != "" {
			return common.BucketToURL(bucket, key), nil
		}
		if checksum, ok := common.CanonicalSHA256(obj.Checksums); ok && strings.TrimSpace(checksum) != "" {
			return common.BucketToURL(bucket, checksum), nil
		}
		return common.BucketToURL(bucket, obj.Id), nil
	}

	existingURL := FirstSupportedAccessURL(obj)
	if strings.TrimSpace(existingURL) == "" {
		return "", fmt.Errorf("object storage location is unavailable")
	}
	return existingURL, nil
}

// ResolveBucket validates a bucket name or returns the default one.
func (m *ObjectManager) ResolveBucket(ctx context.Context, bucketName string) (string, error) {
	creds, err := m.ListS3Credentials(ctx)
	if err != nil {
		return "", err
	}
	return resolveBucketName(creds, bucketName)
}

func (m *ObjectManager) SignDownloadPart(ctx context.Context, bucket, accessURL string, start, end int64, options urlmanager.SignOptions) (string, error) {
	return m.uM.SignDownloadPart(ctx, bucket, accessURL, start, end, options)
}

func (m *ObjectManager) SignObjectDownloadPart(ctx context.Context, obj *models.InternalObject, bucket, accessURL string, start, end int64, options urlmanager.SignOptions) (string, error) {
	scopedURL, err := m.resolveScopedStorageURL(ctx, obj, accessURL)
	if err != nil {
		return "", err
	}
	if b, _, ok := common.ParseS3URL(scopedURL); ok {
		bucket = b
	}
	return m.SignDownloadPart(ctx, bucket, scopedURL, start, end, options)
}

// ResolveObjectRemotePath returns the key for an object in a specific bucket.
func (m *ObjectManager) ResolveObjectRemotePath(ctx context.Context, objectID string, bucket string) (string, bool) {
	obj, err := m.GetObject(ctx, objectID, "")
	if err != nil {
		return "", false
	}
	return S3KeyFromInternalObjectForBucket(obj, bucket)
}

func (m *ObjectManager) resolveSigningBucket(ctx context.Context, accessURL string) string {
	if bucket, _, ok := common.ParseS3URL(accessURL); ok {
		return bucket
	}
	if strings.HasPrefix(accessURL, "s3://") {
		parts := strings.Split(strings.TrimPrefix(accessURL, "s3://"), "/")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	if parsed, err := url.Parse(strings.TrimSpace(accessURL)); err == nil && parsed.Scheme == "" && strings.TrimSpace(parsed.Path) != "" {
		if creds, err := m.ListS3Credentials(ctx); err == nil {
			for _, cred := range creds {
				if common.NormalizeProvider(cred.Provider, "") == common.FileProvider {
					return cred.Bucket
				}
			}
		}
	}
	return ""
}

func (m *ObjectManager) resolveScopedStorageURL(ctx context.Context, obj *models.InternalObject, accessURL string) (string, error) {
	if obj == nil || len(ObjectAccessResources(obj)) == 0 {
		return accessURL, nil
	}
	bucket, key, _ := parseS3Location(accessURL)
	scopes, err := m.bucketScopesForObject(ctx, obj)
	if err != nil {
		return "", err
	}
	if len(scopes) == 0 {
		return accessURL, nil
	}
	targetBucket := strings.TrimSpace(bucket)
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Bucket) != "" {
			targetBucket = strings.TrimSpace(scope.Bucket)
		}
	}
	if targetBucket == "" {
		return accessURL, nil
	}
	targetKey := normalizeScopedStorageKey(key, scopes)
	if strings.Trim(strings.TrimSpace(key), "/") == "" {
		if checksum, ok := common.CanonicalSHA256(obj.Checksums); ok {
			targetKey = normalizeScopedStorageKey(checksum, scopes)
		}
	}
	if targetKey == "" {
		return accessURL, nil
	}
	return common.S3Prefix + targetBucket + "/" + targetKey, nil
}

func (m *ObjectManager) bucketScopeForObjectURL(ctx context.Context, obj *models.InternalObject, bucket string) (models.BucketScope, bool, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return models.BucketScope{}, false, nil
	}
	var fallback *models.BucketScope
	for _, candidate := range sortedObjectScopes(syfoncommon.ControlledAccessToAuthzMap(ObjectAccessResources(obj))) {
		scope, found, err := m.lookupBucketScope(ctx, candidate.organization, candidate.project)
		if err != nil {
			return models.BucketScope{}, false, err
		}
		if !found {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(scope.Bucket), bucket) {
			return scope, true, nil
		}
		if fallback == nil {
			copyScope := scope
			fallback = &copyScope
		}
	}
	if fallback != nil {
		return *fallback, true, nil
	}
	return models.BucketScope{}, false, nil
}

type objectScopeCandidate struct {
	organization string
	project      string
}

func sortedObjectScopes(authz map[string][]string) []objectScopeCandidate {
	candidates := make([]objectScopeCandidate, 0)
	orgs := make([]string, 0, len(authz))
	for org := range authz {
		orgs = append(orgs, org)
	}
	sort.Strings(orgs)
	for _, org := range orgs {
		projects := append([]string(nil), authz[org]...)
		if len(projects) == 0 {
			candidates = append(candidates, objectScopeCandidate{organization: org})
			continue
		}
		sort.Strings(projects)
		for _, project := range projects {
			candidates = append(candidates, objectScopeCandidate{organization: org, project: project})
		}
		candidates = append(candidates, objectScopeCandidate{organization: org})
	}
	return candidates
}

func keyHasStoragePrefix(key, prefix string) bool {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}

func parseS3Location(accessURL string) (bucket string, key string, ok bool) {
	if bucket, key, ok := common.ParseS3URL(accessURL); ok {
		return bucket, key, true
	}
	parsed, err := url.Parse(strings.TrimSpace(accessURL))
	if err != nil {
		return "", "", false
	}
	if !strings.EqualFold(strings.TrimSpace(parsed.Scheme), "s3") {
		return "", "", false
	}
	return strings.TrimSpace(parsed.Host), strings.Trim(strings.TrimSpace(parsed.Path), "/"), true
}

func normalizeScopedStorageKey(key string, scopes []models.BucketScope) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefixes := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
		if prefix == "" {
			continue
		}
		prefixes = append(prefixes, prefix)
	}
	remainder := key
	for _, prefix := range prefixes {
		remainder = trimLeadingStoragePrefix(remainder, prefix)
	}
	composedPrefix := strings.Join(prefixes, "/")
	switch {
	case composedPrefix == "":
		return remainder
	case remainder == "":
		return composedPrefix
	default:
		return path.Join(composedPrefix, remainder)
	}
}

func trimLeadingStoragePrefix(key, prefix string) string {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if key == "" || prefix == "" {
		return key
	}
	if key == prefix {
		return ""
	}
	if strings.HasPrefix(key, prefix+"/") {
		return strings.TrimPrefix(key, prefix+"/")
	}
	return key
}

func (m *ObjectManager) bucketScopesForObject(ctx context.Context, obj *models.InternalObject) ([]models.BucketScope, error) {
	if obj == nil {
		return nil, nil
	}
	authz := syfoncommon.ControlledAccessToAuthzMap(ObjectAccessResources(obj))
	if len(authz) == 0 {
		return nil, nil
	}
	orgs := make([]string, 0, len(authz))
	for org := range authz {
		orgs = append(orgs, org)
	}
	sort.Strings(orgs)
	scopes := make([]models.BucketScope, 0, len(orgs)*2)
	for _, org := range orgs {
		if scope, found, err := m.lookupBucketScope(ctx, org, ""); err != nil {
			return nil, err
		} else if found {
			scopes = append(scopes, scope)
		}
		projects := append([]string(nil), authz[org]...)
		sort.Strings(projects)
		for _, project := range projects {
			scope, found, err := m.lookupBucketScope(ctx, org, project)
			if err != nil {
				return nil, err
			}
			if found {
				scopes = append(scopes, scope)
			}
		}
	}
	return scopes, nil
}

func resolveBucketName(creds []models.S3Credential, bucketName string) (string, error) {
	if len(creds) == 0 {
		return "", fmt.Errorf("no buckets configured")
	}
	if bucketName == "" {
		return creds[0].Bucket, nil
	}
	for _, c := range creds {
		if c.Bucket == bucketName {
			return c.Bucket, nil
		}
	}
	return "", fmt.Errorf("bucket %q not configured", bucketName)
}
