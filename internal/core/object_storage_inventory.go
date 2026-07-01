package core

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

type StorageBucketObject struct {
	ObjectURL   string
	Provider    string
	Bucket      string
	Key         string
	Path        string
	SizeBytes   int64
	MetaSHA256  string
	ETag        string
	LastModTime time.Time
}

type resolvedStorageScopeTarget struct {
	provider string
	bucket   string
	prefix   string
	cred     models.S3Credential
}

type ProjectStorageDeleteResult struct {
	ObjectURL string
	Status    string
	Error     string
}

func (m *ObjectManager) SetS3PrefixLister(fn func(context.Context, models.S3Credential, string, string, bool) ([]StorageBucketObject, error)) {
	if fn == nil {
		m.listS3Prefix = defaultS3PrefixLister
		return
	}
	m.listS3Prefix = fn
}

func (m *ObjectManager) ListProjectStorageObjects(ctx context.Context, organization, project string, includeHead bool) ([]StorageBucketObject, error) {
	ctx = WithStorageInspectCache(ctx)
	target, err := m.resolveProjectStorageScopeTarget(ctx, organization, project)
	if err != nil {
		return nil, err
	}
	items, err := m.listS3Prefix(ctx, target.cred, target.bucket, target.prefix, includeHead)
	if err != nil {
		return nil, err
	}
	out := make([]StorageBucketObject, 0, len(items))
	for _, item := range items {
		entry := item
		entry.Provider = common.S3Provider
		entry.Bucket = target.bucket
		entry.Key = strings.Trim(strings.TrimSpace(entry.Key), "/")
		entry.ObjectURL = common.BucketToURL(target.bucket, entry.Key)
		if strings.TrimSpace(entry.Path) == "" {
			entry.Path = path.Base(entry.Key)
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func (m *ObjectManager) resolveProjectStorageScopeTarget(ctx context.Context, organization, project string) (*resolvedStorageScopeTarget, error) {
	return m.resolveProjectStorageScopeTargetForMethod(ctx, organization, project, objectMethodRead)
}

func (m *ObjectManager) resolveProjectStorageScopeTargetForMethod(ctx context.Context, organization, project, method string) (*resolvedStorageScopeTarget, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "organization is required"}
	}
	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: err.Error()}
	}
	if authz.IsAuthzEnforced(ctx) && !authz.HasMethodAccess(ctx, method, []string{resource}) {
		return nil, &common.AuthorizationError{Method: method, Resources: []string{resource}}
	}

	scopes := make([]models.BucketScope, 0, 2)
	if scope, found, err := m.lookupBucketScope(ctx, organization, ""); err != nil {
		return nil, err
	} else if found {
		scopes = append(scopes, scope)
	}
	if project != "" {
		if scope, found, err := m.lookupBucketScope(ctx, organization, project); err != nil {
			return nil, err
		} else if found {
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		if project != "" {
			return nil, &StorageInspectError{Kind: StorageInspectScopeNotFound, Message: fmt.Sprintf("no bucket scope configured for organization %q project %q", organization, project)}
		}
		return nil, &StorageInspectError{Kind: StorageInspectScopeNotFound, Message: fmt.Sprintf("no bucket scope configured for organization %q", organization)}
	}

	bucket := ""
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Bucket) != "" {
			bucket = strings.TrimSpace(scope.Bucket)
		}
	}
	if bucket == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: fmt.Sprintf("unable to resolve scoped storage bucket for organization %q project %q", organization, project)}
	}
	cred, err := m.credentialForBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if common.NormalizeProvider(cred.Provider, common.S3Provider) != common.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for scoped bucket listing", cred.Provider)}
	}
	return &resolvedStorageScopeTarget{
		provider: common.S3Provider,
		bucket:   bucket,
		prefix:   strings.Join(normalizedScopePrefixes(scopes), "/"),
		cred:     *cred,
	}, nil
}

func (m *ObjectManager) DeleteProjectStorageObjects(ctx context.Context, organization, project string, objectURLs []string) []ProjectStorageDeleteResult {
	ctx = WithStorageInspectCache(ctx)
	normalized := uniqueStorageObjectURLs(objectURLs)
	if len(normalized) == 0 {
		return []ProjectStorageDeleteResult{}
	}
	results := make([]ProjectStorageDeleteResult, 0, len(normalized))
	target, err := m.resolveProjectStorageScopeTargetForMethod(ctx, organization, project, objectMethodDelete)
	if err != nil {
		for _, objectURL := range normalized {
			results = append(results, ProjectStorageDeleteResult{
				ObjectURL: objectURL,
				Status:    "error",
				Error:     err.Error(),
			})
		}
		return results
	}
	for _, objectURL := range normalized {
		result := ProjectStorageDeleteResult{
			ObjectURL: objectURL,
			Status:    "deleted",
		}
		storageTarget, ok, targetErr := m.storageTargetFromURL(ctx, objectURL)
		switch {
		case targetErr != nil:
			result.Status = "error"
			result.Error = targetErr.Error()
		case !ok:
			result.Status = "invalid"
			result.Error = "object_url must resolve to a deletable storage target"
		case !projectStorageTargetAllowed(storageTarget, target):
			result.Status = "forbidden"
			result.Error = fmt.Sprintf("object_url %q is outside configured project bucket scope", objectURL)
		default:
			err := m.deleteStorageTarget(ctx, storageTarget)
			if err == nil {
				break
			}
			result.Status = "error"
			result.Error = err.Error()
		}
		results = append(results, result)
	}
	return results
}

func projectStorageTargetAllowed(candidate storageTarget, target *resolvedStorageScopeTarget) bool {
	if target == nil {
		return false
	}
	if common.NormalizeProvider(candidate.provider, common.S3Provider) != common.S3Provider {
		return false
	}
	if strings.TrimSpace(candidate.bucket) != strings.TrimSpace(target.bucket) {
		return false
	}
	return projectStorageKeyWithinPrefix(candidate.key, target.prefix)
}

func projectStorageKeyWithinPrefix(key, prefix string) bool {
	cleanKey := strings.Trim(strings.TrimSpace(key), "/")
	cleanPrefix := strings.Trim(strings.TrimSpace(prefix), "/")
	if cleanPrefix == "" {
		return cleanKey != ""
	}
	return cleanKey == cleanPrefix || strings.HasPrefix(cleanKey, cleanPrefix+"/")
}

func uniqueStorageObjectURLs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func defaultS3PrefixLister(ctx context.Context, cred models.S3Credential, bucket string, prefix string, includeHead bool) ([]StorageBucketObject, error) {
	client, err := s3ClientFromContext(ctx, cred)
	if err != nil {
		return nil, err
	}
	input := &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	if trimmedPrefix := strings.Trim(strings.TrimSpace(prefix), "/"); trimmedPrefix != "" {
		input.Prefix = aws.String(trimmedPrefix + "/")
	}
	paginator := awss3.NewListObjectsV2Paginator(client, input)
	out := make([]StorageBucketObject, 0)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, classifyS3ListError(bucket, prefix, err)
		}
		for _, item := range page.Contents {
			key := strings.Trim(strings.TrimSpace(aws.ToString(item.Key)), "/")
			if key == "" {
				continue
			}
			var size int64
			if item.Size != nil {
				size = *item.Size
			}
			lastMod := time.Time{}
			if item.LastModified != nil {
				lastMod = *item.LastModified
			}
			out = append(out, StorageBucketObject{
				Provider:    common.S3Provider,
				Bucket:      bucket,
				Key:         key,
				ObjectURL:   common.BucketToURL(bucket, key),
				Path:        path.Base(key),
				SizeBytes:   size,
				ETag:        strings.Trim(strings.TrimSpace(aws.ToString(item.ETag)), "\""),
				LastModTime: lastMod,
			})
		}
	}
	if !includeHead || len(out) == 0 {
		return out, nil
	}

	workers := len(out)
	if workers > maxStorageInspectWorkers {
		workers = maxStorageInspectWorkers
	}
	if workers == 0 {
		return out, nil
	}
	indexCh := make(chan int)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range indexCh {
				meta, err := defaultS3ObjectInspector(ctx, cred, bucket, out[index].Key)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				out[index].MetaSHA256 = strings.TrimSpace(meta.MetaSHA256)
				if out[index].ETag == "" {
					out[index].ETag = strings.TrimSpace(meta.ETag)
				}
				if out[index].LastModTime.IsZero() {
					out[index].LastModTime = meta.LastModTime
				}
			}
		}()
	}
	for index := range out {
		indexCh <- index
	}
	close(indexCh)
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

func classifyS3ListError(bucket string, prefix string, err error) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch strings.ToLower(strings.TrimSpace(apiErr.ErrorCode())) {
		case "forbidden", "accessdenied", "permissiondenied":
			return &StorageInspectError{
				Kind: StorageInspectBucketUnavailable,
				Message: fmt.Sprintf(
					"provider rejected bucket inventory request for s3://%s/%s; mapped bucket target may be missing or inaccessible",
					bucket,
					strings.Trim(strings.TrimSpace(prefix), "/"),
				),
			}
		case "notfound", "nosuchbucket":
			return &StorageInspectError{Kind: StorageInspectObjectNotFound, Message: fmt.Sprintf("provider could not find bucket %q", bucket)}
		}
	}
	return fmt.Errorf("list s3 objects for %s/%s: %w", bucket, strings.Trim(strings.TrimSpace(prefix), "/"), err)
}
