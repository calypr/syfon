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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

type InspectStorageRequest struct {
	Organization string
	Project      string
	Key          string
	Scheme       string
	ObjectURL    string
}

type StorageObjectMetadata struct {
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

type StorageInspectErrorKind string

const (
	StorageInspectInvalidInput      StorageInspectErrorKind = "invalid_input"
	StorageInspectScopeNotFound     StorageInspectErrorKind = "scope_not_found"
	StorageInspectCredentialMissing StorageInspectErrorKind = "credential_missing"
	StorageInspectPermissionDenied  StorageInspectErrorKind = "permission_denied"
	StorageInspectObjectNotFound    StorageInspectErrorKind = "object_not_found"
	StorageInspectUnsupported       StorageInspectErrorKind = "unsupported"
)

type StorageInspectError struct {
	Kind    StorageInspectErrorKind
	Message string
}

var storageInspectCacheKey contextKey = "storageInspectCache"

type storageInspectCredentialCacheEntry struct {
	cred *models.S3Credential
	err  error
}

type storageInspectRequestCache struct {
	mu sync.Mutex

	credentials   map[string]storageInspectCredentialCacheEntry
	visible       map[string]VisibleBucket
	visibleErr    error
	visibleLoaded bool
}

func (e *StorageInspectError) Error() string {
	if e == nil {
		return "storage inspect failed"
	}
	if strings.TrimSpace(e.Message) != "" {
		return e.Message
	}
	return fmt.Sprintf("storage inspect failed: %s", e.Kind)
}



func storageInspectCacheFromContext(ctx context.Context) *storageInspectRequestCache {
	cache, _ := ctx.Value(storageInspectCacheKey).(*storageInspectRequestCache)
	return cache
}

func (m *ObjectManager) SetS3ObjectInspector(fn func(context.Context, models.S3Credential, string, string) (*StorageObjectMetadata, error)) {
	if fn == nil {
		m.inspectS3Object = defaultS3ObjectInspector
		return
	}
	m.inspectS3Object = fn
}

func (m *ObjectManager) InspectStorageObject(ctx context.Context, req InspectStorageRequest) (*StorageObjectMetadata, error) {
	if strings.TrimSpace(req.ObjectURL) != "" {
		return m.inspectRawStorageObject(ctx, req)
	}
	return m.inspectScopedStorageObject(ctx, req)
}

func (m *ObjectManager) inspectScopedStorageObject(ctx context.Context, req InspectStorageRequest) (*StorageObjectMetadata, error) {
	organization := strings.TrimSpace(req.Organization)
	project := strings.TrimSpace(req.Project)
	key := strings.Trim(strings.TrimSpace(req.Key), "/")
	if organization == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "organization is required for scoped object inspection"}
	}
	if key == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "key is required for scoped object inspection"}
	}
	scheme := strings.ToLower(strings.TrimSpace(req.Scheme))
	if scheme == "" {
		scheme = common.S3Provider
	}
	if scheme != common.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider scheme %q is not supported for server-backed add-url inspection", scheme)}
	}

	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: err.Error()}
	}
	if authz.IsAuthzEnforced(ctx) && !authz.HasMethodAccess(ctx, objectMethodRead, []string{resource}) {
		return nil, &common.AuthorizationError{Method: objectMethodRead, Resources: []string{resource}}
	}

	target, err := m.ResolveScopedUploadTarget(ctx, organization, project, key)
	if err != nil {
		if errors.Is(err, common.ErrInvalidInput) && strings.Contains(err.Error(), "no bucket scope configured") {
			return nil, &StorageInspectError{Kind: StorageInspectScopeNotFound, Message: err.Error()}
		}
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: err.Error()}
	}
	cred, err := m.credentialForBucket(ctx, target.Bucket)
	if err != nil {
		return nil, err
	}
	if common.NormalizeProvider(cred.Provider, common.S3Provider) != common.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for server-backed add-url inspection", cred.Provider)}
	}
	meta, err := m.inspectS3Object(ctx, *cred, target.Bucket, target.Key)
	if err != nil {
		return nil, err
	}
	meta.ObjectURL = target.URL
	meta.Provider = common.S3Provider
	meta.Bucket = target.Bucket
	meta.Key = target.Key
	if strings.TrimSpace(meta.Path) == "" {
		meta.Path = path.Base(target.Key)
	}
	return meta, nil
}

func (m *ObjectManager) inspectRawStorageObject(ctx context.Context, req InspectStorageRequest) (*StorageObjectMetadata, error) {
	rawURL := strings.TrimSpace(req.ObjectURL)
	bucket, key, ok := common.ParseS3URL(rawURL)
	if !ok {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "object_url must be a valid s3://bucket/key URL"}
	}
	cred, err := m.credentialForBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	visible, err := m.ListVisibleBuckets(ctx)
	if err != nil {
		return nil, err
	}
	if !bucketVisibleToCaller(visible, bucket, credentialIDForCredential(*cred)) {
		return nil, &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: fmt.Sprintf("bucket %q is not visible to the caller", bucket)}
	}
	if common.NormalizeProvider(cred.Provider, common.S3Provider) != common.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for server-backed add-url inspection", cred.Provider)}
	}
	meta, err := m.inspectS3Object(ctx, *cred, bucket, key)
	if err != nil {
		return nil, err
	}
	meta.ObjectURL = common.BucketToURL(bucket, key)
	meta.Provider = common.S3Provider
	meta.Bucket = bucket
	meta.Key = key
	if strings.TrimSpace(meta.Path) == "" {
		meta.Path = path.Base(key)
	}
	return meta, nil
}

func (m *ObjectManager) credentialForBucket(ctx context.Context, bucket string) (*models.S3Credential, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "bucket is required"}
	}
	if cache := storageInspectCacheFromContext(ctx); cache != nil {
		if cred, err, ok := cache.getCredential(bucket); ok {
			return cred, err
		}
	}
	if cred, err := m.db.GetS3Credential(ctx, bucket); err == nil && cred != nil {
		if cache := storageInspectCacheFromContext(ctx); cache != nil {
			cache.setCredential(bucket, cred, nil)
		}
		return cred, nil
	}
	creds, err := m.db.ListS3Credentials(ctx)
	if err != nil {
		if cache := storageInspectCacheFromContext(ctx); cache != nil {
			cache.setCredential(bucket, nil, err)
		}
		return nil, err
	}
	for _, cred := range creds {
		if strings.EqualFold(strings.TrimSpace(cred.Bucket), bucket) || strings.EqualFold(strings.TrimSpace(cred.CredentialID), bucket) {
			copy := cred
			if cache := storageInspectCacheFromContext(ctx); cache != nil {
				cache.setCredential(bucket, &copy, nil)
			}
			return &copy, nil
		}
	}
	err = &StorageInspectError{Kind: StorageInspectCredentialMissing, Message: fmt.Sprintf("no stored bucket credential found for bucket %q", bucket)}
	if cache := storageInspectCacheFromContext(ctx); cache != nil {
		cache.setCredential(bucket, nil, err)
	}
	return nil, err
}

func bucketVisibleToCaller(visible map[string]VisibleBucket, bucket string, credentialID string) bool {
	for key, entry := range visible {
		if strings.EqualFold(strings.TrimSpace(entry.Credential.Bucket), bucket) || strings.EqualFold(strings.TrimSpace(key), credentialID) || strings.EqualFold(strings.TrimSpace(entry.Credential.CredentialID), credentialID) {
			return true
		}
	}
	return false
}

func (m *ObjectManager) listVisibleBucketsCached(ctx context.Context) (map[string]VisibleBucket, error) {
	if cache := storageInspectCacheFromContext(ctx); cache != nil {
		if visible, err, ok := cache.getVisible(); ok {
			return visible, err
		}
	}
	visible, err := m.listVisibleBucketsUncached(ctx)
	if cache := storageInspectCacheFromContext(ctx); cache != nil {
		cache.setVisible(visible, err)
	}
	return visible, err
}

func (c *storageInspectRequestCache) getCredential(bucket string) (*models.S3Credential, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.credentials[strings.ToLower(strings.TrimSpace(bucket))]
	if !ok {
		return nil, nil, false
	}
	if entry.cred == nil {
		return nil, entry.err, true
	}
	copy := *entry.cred
	return &copy, entry.err, true
}

func (c *storageInspectRequestCache) setCredential(bucket string, cred *models.S3Credential, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := strings.ToLower(strings.TrimSpace(bucket))
	if cred == nil {
		c.credentials[key] = storageInspectCredentialCacheEntry{err: err}
		return
	}
	copy := *cred
	c.credentials[key] = storageInspectCredentialCacheEntry{cred: &copy, err: err}
}

func (c *storageInspectRequestCache) getVisible() (map[string]VisibleBucket, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.visibleLoaded {
		return nil, nil, false
	}
	return cloneVisibleBuckets(c.visible), c.visibleErr, true
}

func (c *storageInspectRequestCache) setVisible(visible map[string]VisibleBucket, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.visible = cloneVisibleBuckets(visible)
	c.visibleErr = err
	c.visibleLoaded = true
}

func cloneVisibleBuckets(in map[string]VisibleBucket) map[string]VisibleBucket {
	if in == nil {
		return nil
	}
	out := make(map[string]VisibleBucket, len(in))
	for key, bucket := range in {
		programs := append([]string(nil), bucket.Programs...)
		sort.Strings(programs)
		out[key] = VisibleBucket{
			Credential: bucket.Credential,
			Programs:   programs,
		}
	}
	return out
}

func defaultS3ObjectInspector(ctx context.Context, cred models.S3Credential, bucket string, key string) (*StorageObjectMetadata, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cred.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cred.AccessKey, cred.SecretKey, "")),
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if endpoint := strings.TrimSpace(cred.Endpoint); endpoint != "" {
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			if strings.Contains(endpoint, "localhost") || strings.Contains(endpoint, "127.0.0.1") {
				endpoint = "http://" + endpoint
			} else {
				endpoint = "https://" + endpoint
			}
		}
		cfg.BaseEndpoint = aws.String(endpoint)
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		if strings.TrimSpace(cred.Endpoint) != "" {
			o.UsePathStyle = true
		}
	})
	out, err := client.HeadObject(ctx, &awss3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch strings.ToLower(strings.TrimSpace(apiErr.ErrorCode())) {
			case "forbidden", "accessdenied", "permissiondenied":
				return nil, &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: fmt.Sprintf("provider denied access to s3://%s/%s", bucket, key)}
			case "notfound", "nosuchkey", "nosuchbucket":
				return nil, &StorageInspectError{Kind: StorageInspectObjectNotFound, Message: fmt.Sprintf("provider could not find s3://%s/%s", bucket, key)}
			}
		}
		return nil, fmt.Errorf("inspect s3 object %s/%s: %w", bucket, key, err)
	}
	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	lastMod := time.Time{}
	if out.LastModified != nil {
		lastMod = *out.LastModified
	}
	return &StorageObjectMetadata{
		Provider:    common.S3Provider,
		Bucket:      bucket,
		Key:         key,
		ObjectURL:   common.BucketToURL(bucket, key),
		Path:        path.Base(key),
		SizeBytes:   size,
		ETag:        strings.Trim(strings.TrimSpace(aws.ToString(out.ETag)), "\""),
		LastModTime: lastMod,
	}, nil
}
