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
	bucket, key, ok := common.ParseS3URL(accessURL)
	if !ok || obj == nil || len(ObjectAccessResources(obj)) == 0 {
		return accessURL, nil
	}
	scope, ok, err := m.bucketScopeForObjectURL(ctx, obj, bucket)
	if err != nil {
		return "", err
	}
	if !ok {
		return accessURL, nil
	}
	targetBucket := strings.TrimSpace(scope.Bucket)
	if targetBucket == "" {
		targetBucket = bucket
	}
	prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
	if prefix == "" {
		if strings.EqualFold(targetBucket, bucket) {
			return accessURL, nil
		}
		return common.S3Prefix + targetBucket + "/" + strings.Trim(key, "/"), nil
	}
	if keyHasStoragePrefix(key, prefix) {
		if strings.EqualFold(targetBucket, bucket) {
			return accessURL, nil
		}
		return common.S3Prefix + targetBucket + "/" + strings.Trim(key, "/"), nil
	}
	return common.S3Prefix + targetBucket + "/" + path.Join(prefix, strings.Trim(key, "/")), nil
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
