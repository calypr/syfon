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

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
)

type InspectStorageRequest struct {
	ID                string
	Organization      string
	Project           string
	Key               string
	Scheme            string
	ObjectURL         string
	ExpectedSizeBytes *int64
	ExpectedSHA256    string
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

type StorageProbeStatus string

const (
	StorageProbeStatusPresent     StorageProbeStatus = "present"
	StorageProbeStatusNotFound    StorageProbeStatus = "not_found"
	StorageProbeStatusForbidden   StorageProbeStatus = "forbidden"
	StorageProbeStatusInvalid     StorageProbeStatus = "invalid"
	StorageProbeStatusUnsupported StorageProbeStatus = "unsupported"
	StorageProbeStatusError       StorageProbeStatus = "error"
)

type StorageValidationStatus string

const (
	StorageValidationNotRequested StorageValidationStatus = "not_requested"
	StorageValidationMatched      StorageValidationStatus = "matched"
	StorageValidationMismatched   StorageValidationStatus = "mismatched"
	StorageValidationUnverifiable StorageValidationStatus = "unverifiable"
)

type StorageProbeResult struct {
	ID                   string
	ObjectURL            string
	Provider             string
	Bucket               string
	Key                  string
	Path                 string
	Exists               bool
	Status               StorageProbeStatus
	Error                string
	ErrorKind            string
	SizeBytes            *int64
	MetaSHA256           string
	ETag                 string
	LastModTime          time.Time
	ValidationStatus     StorageValidationStatus
	SizeMatch            *bool
	SHA256Match          *bool
	ValidationMismatches []string
}

type StorageInspectErrorKind string

const (
	StorageInspectInvalidInput      StorageInspectErrorKind = "invalid_input"
	StorageInspectScopeNotFound     StorageInspectErrorKind = "scope_not_found"
	StorageInspectCredentialMissing StorageInspectErrorKind = "credential_missing"
	StorageInspectPermissionDenied  StorageInspectErrorKind = "permission_denied"
	StorageInspectObjectNotFound    StorageInspectErrorKind = "object_not_found"
	StorageInspectBucketUnavailable StorageInspectErrorKind = "bucket_unavailable"
	StorageInspectListingIncomplete StorageInspectErrorKind = "listing_incomplete"
	StorageInspectUnsupported       StorageInspectErrorKind = "unsupported"
)

const maxStorageInspectWorkers = 8

type StorageInspectError struct {
	Kind    StorageInspectErrorKind
	Message string
}

type storageInspectContextKey string

var storageInspectCacheKey storageInspectContextKey = "storageInspectCache"

type storageInspectCredentialCacheEntry struct {
	cred *buckets.Credential
	err  error
}

type storageInspectRequestCache struct {
	mu sync.Mutex

	credentials   map[string]storageInspectCredentialCacheEntry
	visible       map[string]buckets.VisibleBucket
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

func WithStorageInspectCache(ctx context.Context) context.Context {
	if storageInspectCacheFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, storageInspectCacheKey, &storageInspectRequestCache{
		credentials: map[string]storageInspectCredentialCacheEntry{},
	})
}

func storageInspectCacheFromContext(ctx context.Context) *storageInspectRequestCache {
	cache, _ := ctx.Value(storageInspectCacheKey).(*storageInspectRequestCache)
	return cache
}

func (m *ObjectManager) InspectStorageObject(ctx context.Context, req InspectStorageRequest) (*StorageObjectMetadata, error) {
	if strings.TrimSpace(req.ObjectURL) != "" {
		return m.inspectRawStorageObject(ctx, req)
	}
	return m.inspectScopedStorageObject(ctx, req)
}

func (m *ObjectManager) InspectStorageObjects(ctx context.Context, items []InspectStorageRequest) []StorageProbeResult {
	ctx = WithStorageInspectCache(ctx)
	if len(items) == 0 {
		return []StorageProbeResult{}
	}
	results := make([]StorageProbeResult, len(items))
	workers := len(items)
	if workers > maxStorageInspectWorkers {
		workers = maxStorageInspectWorkers
	}
	type probeWork struct {
		index int
		req   InspectStorageRequest
	}
	workCh := make(chan probeWork)
	cache := newStorageProbeResultCache()
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				results[work.index] = cache.probe(ctx, m, work.req)
			}
		}()
	}
	for i, item := range items {
		workCh <- probeWork{index: i, req: item}
	}
	close(workCh)
	wg.Wait()
	return results
}

type storageProbeResultCache struct {
	mu      sync.Mutex
	results map[string]StorageProbeResult
}

func newStorageProbeResultCache() *storageProbeResultCache {
	return &storageProbeResultCache{results: map[string]StorageProbeResult{}}
}

func (c *storageProbeResultCache) probe(ctx context.Context, m *ObjectManager, req InspectStorageRequest) StorageProbeResult {
	key := storageProbeCacheKey(req)
	c.mu.Lock()
	if result, ok := c.results[key]; ok {
		c.mu.Unlock()
		return cloneStorageProbeResultForRequest(result, req)
	}
	c.mu.Unlock()

	result := m.inspectStorageProbe(ctx, req)

	c.mu.Lock()
	c.results[key] = result
	c.mu.Unlock()
	return result
}

func storageProbeCacheKey(req InspectStorageRequest) string {
	return strings.TrimSpace(req.ObjectURL) + "|" +
		strings.TrimSpace(req.Organization) + "|" +
		strings.TrimSpace(req.Project) + "|" +
		strings.TrimSpace(req.Key) + "|" +
		strings.TrimSpace(req.Scheme) + "|" +
		normalizeExpectedSize(req.ExpectedSizeBytes) + "|" +
		strings.ToLower(strings.TrimSpace(strings.TrimPrefix(req.ExpectedSHA256, "sha256:")))
}

func normalizeExpectedSize(v *int64) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%d", *v)
}

func cloneStorageProbeResultForRequest(result StorageProbeResult, req InspectStorageRequest) StorageProbeResult {
	clone := result
	clone.ID = strings.TrimSpace(req.ID)
	if clone.ID == "" {
		clone.ID = strings.TrimSpace(result.ID)
	}
	if clone.ObjectURL == "" {
		clone.ObjectURL = strings.TrimSpace(req.ObjectURL)
	}
	clone.ValidationMismatches = append([]string(nil), result.ValidationMismatches...)
	return clone
}

func (m *ObjectManager) inspectStorageProbe(ctx context.Context, req InspectStorageRequest) StorageProbeResult {
	result := StorageProbeResult{
		ID:               strings.TrimSpace(req.ID),
		ObjectURL:        strings.TrimSpace(req.ObjectURL),
		Status:           StorageProbeStatusError,
		ValidationStatus: StorageValidationNotRequested,
	}
	meta, err := m.InspectStorageObject(ctx, req)
	if err != nil {
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.Exists = false
		result.ValidationStatus = storageValidationStatusForError(req)
		return result
	}
	result.ObjectURL = strings.TrimSpace(meta.ObjectURL)
	result.Provider = strings.TrimSpace(meta.Provider)
	result.Bucket = strings.TrimSpace(meta.Bucket)
	result.Key = strings.TrimSpace(meta.Key)
	result.Path = strings.TrimSpace(meta.Path)
	result.Exists = true
	result.Status = StorageProbeStatusPresent
	result.SizeBytes = ptrInt64(meta.SizeBytes)
	result.MetaSHA256 = strings.TrimSpace(meta.MetaSHA256)
	result.ETag = strings.TrimSpace(meta.ETag)
	result.LastModTime = meta.LastModTime
	result.ValidationStatus, result.SizeMatch, result.SHA256Match, result.ValidationMismatches = validateStorageProbe(req, meta)
	return result
}

func classifyStorageProbeError(err error) (StorageProbeStatus, string) {
	var inspectErr *StorageInspectError
	if errors.As(err, &inspectErr) {
		switch inspectErr.Kind {
		case StorageInspectObjectNotFound:
			return StorageProbeStatusNotFound, string(inspectErr.Kind)
		case StorageInspectPermissionDenied, StorageInspectBucketUnavailable:
			return StorageProbeStatusForbidden, string(inspectErr.Kind)
		case StorageInspectInvalidInput, StorageInspectScopeNotFound, StorageInspectCredentialMissing:
			return StorageProbeStatusInvalid, string(inspectErr.Kind)
		case StorageInspectUnsupported:
			return StorageProbeStatusUnsupported, string(inspectErr.Kind)
		default:
			return StorageProbeStatusError, string(inspectErr.Kind)
		}
	}
	return StorageProbeStatusError, "error"
}

func storageValidationStatusForError(req InspectStorageRequest) StorageValidationStatus {
	if req.ExpectedSizeBytes == nil && strings.TrimSpace(req.ExpectedSHA256) == "" {
		return StorageValidationNotRequested
	}
	return StorageValidationUnverifiable
}

func validateStorageProbe(req InspectStorageRequest, meta *StorageObjectMetadata) (StorageValidationStatus, *bool, *bool, []string) {
	if req.ExpectedSizeBytes == nil && strings.TrimSpace(req.ExpectedSHA256) == "" {
		return StorageValidationNotRequested, nil, nil, nil
	}
	mismatches := make([]string, 0, 2)
	var sizeMatch *bool
	if req.ExpectedSizeBytes != nil {
		matched := meta.SizeBytes == *req.ExpectedSizeBytes
		sizeMatch = &matched
		if !matched {
			mismatches = append(mismatches, "size_mismatch")
		}
	}
	var shaMatch *bool
	expectedSHA := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(req.ExpectedSHA256, "sha256:")))
	if expectedSHA != "" {
		if strings.TrimSpace(meta.MetaSHA256) == "" {
			return StorageValidationUnverifiable, sizeMatch, nil, append(mismatches, "missing_remote_sha256")
		}
		matched := strings.EqualFold(strings.TrimSpace(meta.MetaSHA256), expectedSHA)
		shaMatch = &matched
		if !matched {
			mismatches = append(mismatches, "sha256_mismatch")
		}
	}
	if len(mismatches) > 0 {
		return StorageValidationMismatched, sizeMatch, shaMatch, mismatches
	}
	return StorageValidationMatched, sizeMatch, shaMatch, nil
}

func ptrInt64(v int64) *int64 {
	copy := v
	return &copy
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
		scheme = address.S3Provider
	}
	if scheme != address.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider scheme %q is not supported for server-backed add-url inspection", scheme)}
	}

	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: err.Error()}
	}
	if access.IsAuthzEnforced(ctx) && !access.HasMethodAccess(ctx, objectMethodRead, []string{resource}) {
		return nil, &access.AuthorizationError{Method: objectMethodRead, Resources: []string{resource}}
	}

	target, err := m.resolveScopedUploadTarget(ctx, organization, project, key)
	if err != nil {
		if errors.Is(err, faults.ErrInvalidInput) && strings.Contains(err.Error(), "no bucket scope configured") {
			return nil, &StorageInspectError{Kind: StorageInspectScopeNotFound, Message: err.Error()}
		}
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: err.Error()}
	}
	cred, err := m.credentialForBucket(ctx, target.Bucket)
	if err != nil {
		return nil, err
	}
	if address.NormalizeProvider(cred.Provider, address.S3Provider) != address.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for server-backed add-url inspection", cred.Provider)}
	}
	meta, err := m.probeStorageObject(ctx, target.Bucket, target.Key)
	if err != nil {
		return nil, err
	}
	meta.ObjectURL = target.URL
	meta.Provider = address.S3Provider
	meta.Bucket = target.Bucket
	meta.Key = target.Key
	if strings.TrimSpace(meta.Path) == "" {
		meta.Path = path.Base(target.Key)
	}
	return meta, nil
}

func (m *ObjectManager) inspectRawStorageObject(ctx context.Context, req InspectStorageRequest) (*StorageObjectMetadata, error) {
	rawURL := strings.TrimSpace(req.ObjectURL)
	bucket, key, ok := address.ParseS3URL(rawURL)
	if !ok {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "object_url must be a valid s3://bucket/key URL"}
	}
	cred, err := m.credentialForBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	visible, err := m.listVisibleBucketsCached(ctx)
	if err != nil {
		return nil, err
	}
	if !buckets.VisibleToCaller(visible, bucket, cred.CredentialID) {
		return nil, &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: fmt.Sprintf("bucket %q is not visible to the caller", bucket)}
	}
	if address.NormalizeProvider(cred.Provider, address.S3Provider) != address.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for server-backed add-url inspection", cred.Provider)}
	}
	meta, err := m.probeStorageObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	meta.ObjectURL = address.BucketToURL(bucket, key)
	meta.Provider = address.S3Provider
	meta.Bucket = bucket
	meta.Key = key
	if strings.TrimSpace(meta.Path) == "" {
		meta.Path = path.Base(key)
	}
	return meta, nil
}

func (m *ObjectManager) probeStorageObject(ctx context.Context, bucket, key string) (*StorageObjectMetadata, error) {
	if m.storageProbe == nil {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: "storage probe is not configured"}
	}
	results := m.storageProbe.Probe(ctx, []storage.ProbeTarget{{
		ID:     "object",
		Target: storage.ObjectTarget{Bucket: bucket, Key: key},
	}})
	if len(results) == 0 {
		return nil, &StorageInspectError{Kind: StorageInspectBucketUnavailable, Message: "storage probe returned no result"}
	}
	result := results[0]
	if result.Err != nil {
		return nil, mapStorageOperationError(result.Err, "probe", bucket, key)
	}
	meta := result.Metadata
	return &StorageObjectMetadata{
		Provider:    strings.TrimSpace(meta.Provider),
		Bucket:      strings.TrimSpace(meta.Bucket),
		Key:         strings.TrimSpace(meta.Key),
		Path:        strings.TrimSpace(meta.Path),
		SizeBytes:   meta.SizeBytes,
		MetaSHA256:  strings.TrimSpace(meta.MetaSHA256),
		ETag:        strings.TrimSpace(meta.ETag),
		LastModTime: meta.LastModified,
	}, nil
}

func (m *ObjectManager) credentialForBucket(ctx context.Context, bucket string) (*buckets.Credential, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, &StorageInspectError{Kind: StorageInspectInvalidInput, Message: "bucket is required"}
	}
	if cache := storageInspectCacheFromContext(ctx); cache != nil {
		if cred, err, ok := cache.getCredential(bucket); ok {
			return cred, err
		}
	}
	if cred, err := m.bucketService.GetS3Credential(ctx, bucket); err == nil && cred != nil {
		if cache := storageInspectCacheFromContext(ctx); cache != nil {
			cache.setCredential(bucket, cred, nil)
		}
		return cred, nil
	}
	creds, err := m.bucketService.ListS3Credentials(ctx)
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

func (m *ObjectManager) listVisibleBucketsCached(ctx context.Context) (map[string]buckets.VisibleBucket, error) {
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

func (c *storageInspectRequestCache) getCredential(bucket string) (*buckets.Credential, error, bool) {
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

func (c *storageInspectRequestCache) setCredential(bucket string, cred *buckets.Credential, err error) {
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

func (c *storageInspectRequestCache) getVisible() (map[string]buckets.VisibleBucket, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.visibleLoaded {
		return nil, nil, false
	}
	return cloneVisibleBuckets(c.visible), c.visibleErr, true
}

func (c *storageInspectRequestCache) setVisible(visible map[string]buckets.VisibleBucket, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.visible = cloneVisibleBuckets(visible)
	c.visibleErr = err
	c.visibleLoaded = true
}

func mapStorageOperationError(err error, capability, bucket, key string) error {
	var operation *storage.OperationError
	if !errors.As(err, &operation) {
		return err
	}
	location := strings.TrimSpace(bucket)
	if strings.TrimSpace(key) != "" {
		location += "/" + strings.Trim(strings.TrimSpace(key), "/")
	}
	message := strings.TrimSpace(operation.Error())
	if message == "" {
		message = fmt.Sprintf("storage %s failed for %s", capability, location)
	}
	switch operation.Kind {
	case storage.ErrorInvalid:
		return &StorageInspectError{Kind: StorageInspectInvalidInput, Message: message}
	case storage.ErrorNotFound:
		if strings.TrimSpace(operation.Provider) == "" {
			return &StorageInspectError{Kind: StorageInspectCredentialMissing, Message: fmt.Sprintf("no stored bucket credential found for bucket %q", bucket)}
		}
		return &StorageInspectError{Kind: StorageInspectObjectNotFound, Message: message}
	case storage.ErrorForbidden:
		return &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: message}
	case storage.ErrorUnavailable:
		return &StorageInspectError{Kind: StorageInspectBucketUnavailable, Message: message}
	case storage.ErrorIncomplete:
		return &StorageInspectError{Kind: StorageInspectListingIncomplete, Message: message}
	case storage.ErrorUnsupported:
		return &StorageInspectError{Kind: StorageInspectUnsupported, Message: message}
	case storage.ErrorProvider:
		return err
	default:
		return err
	}
}

func cloneVisibleBuckets(in map[string]buckets.VisibleBucket) map[string]buckets.VisibleBucket {
	if in == nil {
		return nil
	}
	out := make(map[string]buckets.VisibleBucket, len(in))
	for key, visible := range in {
		programs := append([]string(nil), visible.Programs...)
		sort.Strings(programs)
		out[key] = buckets.VisibleBucket{
			Credential: visible.Credential,
			Programs:   programs,
		}
	}
	return out
}
