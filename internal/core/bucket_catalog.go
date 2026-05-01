package core

import (
	"context"
	"net/url"
	"path/filepath"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
)

func (m *ObjectManager) ListS3Credentials(ctx context.Context) ([]models.S3Credential, error) {
	return m.db.ListS3Credentials(ctx)
}

func (m *ObjectManager) GetS3Credential(ctx context.Context, bucket string) (*models.S3Credential, error) {
	return m.db.GetS3Credential(ctx, bucket)
}

func (m *ObjectManager) SaveS3Credential(ctx context.Context, cred *models.S3Credential) error {
	if err := m.db.SaveS3Credential(ctx, cred); err != nil {
		return err
	}
	m.invalidateBucketSignerCache(cred.Bucket)
	return nil
}

func (m *ObjectManager) DeleteS3Credential(ctx context.Context, bucket string) error {
	if err := m.db.DeleteS3Credential(ctx, bucket); err != nil {
		return err
	}
	m.bucketScopeCache.clear()
	m.invalidateBucketSignerCache(bucket)
	return nil
}

func (m *ObjectManager) invalidateBucketSignerCache(bucket string) {
	invalidator, ok := m.uM.(urlmanager.BucketCacheInvalidator)
	if !ok {
		return
	}
	invalidator.InvalidateBucket(bucket)
}

func (m *ObjectManager) ListBucketScopes(ctx context.Context) ([]models.BucketScope, error) {
	return m.db.ListBucketScopes(ctx)
}

func (m *ObjectManager) ListVisibleBuckets(ctx context.Context) (map[string]VisibleBucket, error) {
	creds, err := m.db.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	if len(creds) == 0 {
		return map[string]VisibleBucket{}, nil
	}

	if lister, ok := m.db.(db.BucketVisibilityLister); ok {
		return m.listVisibleBucketsFromRows(ctx, lister, creds)
	}

	objects, err := m.listBucketsVisibleObjects(ctx)
	if err != nil {
		return nil, err
	}

	byBucket := make(map[string]VisibleBucket, len(creds))
	programsSeen := make(map[string]map[string]struct{}, len(creds))
	for _, cred := range creds {
		byBucket[cred.Bucket] = VisibleBucket{Credential: cred}
		programsSeen[cred.Bucket] = map[string]struct{}{}
	}

	for _, obj := range objects {
		programs := ObjectAccessResources(&obj)
		if obj.AccessMethods == nil {
			continue
		}
		for _, method := range *obj.AccessMethods {
			bucket, ok := bucketForAccessMethod(method, creds)
			if !ok {
				continue
			}
			entry, exists := byBucket[bucket]
			if !exists {
				continue
			}
			for _, program := range programs {
				if program == "" {
					continue
				}
				if _, seen := programsSeen[bucket][program]; seen {
					continue
				}
				programsSeen[bucket][program] = struct{}{}
				entry.Programs = append(entry.Programs, program)
			}
			byBucket[bucket] = entry
		}
	}

	filtered := make(map[string]VisibleBucket)
	for bucket, entry := range byBucket {
		if len(programsSeen[bucket]) == 0 && !bucketReferencedByPublicObject(objects, entry.Credential, creds) {
			continue
		}
		sort.Strings(entry.Programs)
		filtered[bucket] = entry
	}
	return filtered, nil
}

func (m *ObjectManager) listVisibleBucketsFromRows(ctx context.Context, lister db.BucketVisibilityLister, creds []models.S3Credential) (map[string]VisibleBucket, error) {
	restrictToResources := authz.IsAuthzEnforced(ctx) &&
		!authz.HasMethodAccess(ctx, objectMethodRead, []string{"/programs"}) &&
		!authz.HasMethodAccess(ctx, objectMethodRead, []string{"/data_file"})
	rows, err := lister.ListBucketVisibilityRows(ctx, readableResources(ctx), true, restrictToResources)
	if err != nil {
		return nil, err
	}

	byBucket := make(map[string]VisibleBucket, len(creds))
	programsSeen := make(map[string]map[string]struct{}, len(creds))
	publicSeen := make(map[string]bool, len(creds))
	for _, cred := range creds {
		byBucket[cred.Bucket] = VisibleBucket{Credential: cred}
		programsSeen[cred.Bucket] = map[string]struct{}{}
	}

	for _, row := range rows {
		methodType := drs.AccessMethodType(strings.TrimSpace(row.AccessType))
		method := drs.AccessMethod{
			Type: methodType,
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: row.AccessURL},
		}
		bucket, ok := bucketForAccessMethod(method, creds)
		if !ok {
			continue
		}
		entry, exists := byBucket[bucket]
		if !exists {
			continue
		}
		resource := strings.TrimSpace(row.Resource)
		if resource == "" {
			publicSeen[bucket] = true
			continue
		}
		if _, seen := programsSeen[bucket][resource]; seen {
			continue
		}
		programsSeen[bucket][resource] = struct{}{}
		entry.Programs = append(entry.Programs, resource)
		byBucket[bucket] = entry
	}

	filtered := make(map[string]VisibleBucket)
	for bucket, entry := range byBucket {
		if len(entry.Programs) == 0 && !publicSeen[bucket] {
			continue
		}
		sort.Strings(entry.Programs)
		filtered[bucket] = entry
	}
	return filtered, nil
}

func (m *ObjectManager) CreateBucketScope(ctx context.Context, scope *models.BucketScope) error {
	if err := m.db.CreateBucketScope(ctx, scope); err != nil {
		return err
	}
	m.bucketScopeCache.set(normalizeBucketScope(scope), true)
	return nil
}

func (m *ObjectManager) listBucketsVisibleObjects(ctx context.Context) ([]models.InternalObject, error) {
	ids, err := m.db.ListObjectIDsByScope(ctx, "", "")
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []models.InternalObject{}, nil
	}
	objects, err := m.db.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}
	if !authz.IsAuthzEnforced(ctx) || authz.HasMethodAccess(ctx, objectMethodRead, []string{"/programs"}) || authz.HasMethodAccess(ctx, objectMethodRead, []string{"/data_file"}) {
		return objects, nil
	}
	return m.filterObjectsByMethod(ctx, objects, objectMethodRead), nil
}

func bucketReferencedByPublicObject(objects []models.InternalObject, cred models.S3Credential, creds []models.S3Credential) bool {
	for _, obj := range objects {
		if len(ObjectAccessResources(&obj)) != 0 || obj.AccessMethods == nil {
			continue
		}
		for _, method := range *obj.AccessMethods {
			if bucket, ok := bucketForAccessMethod(method, creds); ok && bucket == cred.Bucket {
				return true
			}
		}
	}
	return false
}

func bucketForAccessMethod(method drs.AccessMethod, creds []models.S3Credential) (string, bool) {
	if method.AccessUrl == nil {
		return "", false
	}
	raw := strings.TrimSpace(method.AccessUrl.Url)
	if raw == "" {
		return "", false
	}
	if bucket, _, ok := common.ParseS3URL(raw); ok {
		return bucket, true
	}
	scheme := common.SchemeFromURL(raw)
	if provider := common.ProviderFromScheme(scheme); provider == common.GCSProvider || provider == common.AzureProvider {
		if parsed, err := url.Parse(raw); err == nil && strings.TrimSpace(parsed.Host) != "" {
			return strings.TrimSpace(parsed.Host), true
		}
	}
	cleanRaw := filepath.Clean(strings.TrimSpace(raw))
	for _, cred := range creds {
		if common.NormalizeProvider(cred.Provider, common.S3Provider) != common.FileProvider {
			continue
		}
		root := strings.TrimSpace(cred.Endpoint)
		if root == "" {
			root = strings.TrimSpace(cred.Bucket)
		}
		if root == "" {
			continue
		}
		cleanRoot := filepath.Clean(root)
		if cleanRaw == cleanRoot || strings.HasPrefix(cleanRaw, cleanRoot+string(filepath.Separator)) {
			return cred.Bucket, true
		}
	}
	return "", false
}

func ObjectAccessResources(obj *models.InternalObject) []string {
	if obj == nil {
		return nil
	}
	if obj.ControlledAccess != nil {
		return syfoncommon.NormalizeAccessResources(*obj.ControlledAccess)
	}
	return syfoncommon.AuthzMapToList(obj.Authorizations)
}

func (m *ObjectManager) lookupBucketScope(ctx context.Context, organization, project string) (models.BucketScope, bool, error) {
	if scope, found, cached := m.bucketScopeCache.get(organization, project); cached {
		return scope, found, nil
	}

	scope, err := m.db.GetBucketScope(ctx, organization, project)
	if err != nil {
		if common.IsNotFoundError(err) {
			m.bucketScopeCache.set(models.BucketScope{Organization: organization, ProjectID: project}, false)
			return models.BucketScope{}, false, nil
		}
		return models.BucketScope{}, false, err
	}
	if scope == nil {
		m.bucketScopeCache.set(models.BucketScope{Organization: organization, ProjectID: project}, false)
		return models.BucketScope{}, false, nil
	}

	normalized := normalizeBucketScope(scope)
	m.bucketScopeCache.set(normalized, true)
	return normalized, true, nil
}
