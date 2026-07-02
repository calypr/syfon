package core

import (
	"context"
	"errors"
	"fmt"
	"log"
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

type StoragePrefixListOptions struct {
	IncludeHead bool
	ExactPrefix bool
	MaxKeys     int32
}

type ProjectStorageInspectMode string

const (
	ProjectStorageInspectItems   ProjectStorageInspectMode = "items"
	ProjectStorageInspectExists  ProjectStorageInspectMode = "exists"
	ProjectStorageInspectSummary ProjectStorageInspectMode = "summary"
)

type ProjectStorageSummary struct {
	Provider    string
	Bucket      string
	Prefix      string
	ObjectURL   string
	Exists      bool
	ObjectCount int
	TotalBytes  int64
	ComputedAt  time.Time
	Mode        ProjectStorageInspectMode
}

type ProjectStorageInspectResult struct {
	Summary ProjectStorageSummary
	Items   []StorageBucketObject
}

type ProjectStorageInspectOptions struct {
	Mode        ProjectStorageInspectMode
	IncludeHead bool
	PathPrefix  string
}

type StorageListValidationRequest struct {
	ID                string
	ObjectURL         string
	ExpectedSizeBytes *int64
	ExpectedName      string
}

type StorageListValidationResult struct {
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
	ETag                 string
	LastModTime          time.Time
	ValidationStatus     StorageValidationStatus
	SizeMatch            *bool
	NameMatch            *bool
	ValidationMismatches []string
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
	m.listS3Prefix = func(ctx context.Context, cred models.S3Credential, bucket string, prefix string, options StoragePrefixListOptions) ([]StorageBucketObject, error) {
		return fn(ctx, cred, bucket, prefix, options.IncludeHead)
	}
}

func (m *ObjectManager) SetS3PrefixListerWithOptions(fn func(context.Context, models.S3Credential, string, string, StoragePrefixListOptions) ([]StorageBucketObject, error)) {
	if fn == nil {
		m.listS3Prefix = defaultS3PrefixLister
		return
	}
	m.listS3Prefix = fn
}

func (m *ObjectManager) ListProjectStorageObjects(ctx context.Context, organization, project string, includeHead bool) ([]StorageBucketObject, error) {
	result, err := m.InspectProjectStorage(ctx, organization, project, ProjectStorageInspectOptions{Mode: ProjectStorageInspectItems, IncludeHead: includeHead})
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (m *ObjectManager) InspectProjectStorage(ctx context.Context, organization, project string, inspectOptions ProjectStorageInspectOptions) (*ProjectStorageInspectResult, error) {
	started := time.Now()
	ctx = WithStorageInspectCache(ctx)
	target, err := m.resolveProjectStorageScopeTarget(ctx, organization, project)
	if err != nil {
		log.Printf("INFO: syfon_project_storage_inspect_done organization=%s project=%s mode=%s path_prefix=%q duration_ms=%d error=%q", organization, project, inspectOptions.Mode, inspectOptions.PathPrefix, time.Since(started).Milliseconds(), err.Error())
		return nil, err
	}
	target = target.withPathPrefix(inspectOptions.PathPrefix)
	normalizedMode := normalizeProjectStorageInspectMode(inspectOptions.Mode)
	options := StoragePrefixListOptions{IncludeHead: inspectOptions.IncludeHead}
	if normalizedMode == ProjectStorageInspectExists {
		options.MaxKeys = 1
	}
	items, err := m.listS3Prefix(ctx, target.cred, target.bucket, target.prefix, options)
	if err != nil {
		log.Printf("INFO: syfon_project_storage_inspect_done organization=%s project=%s mode=%s path_prefix=%q bucket=%s prefix=%q max_keys=%d include_head=%t duration_ms=%d error=%q", organization, project, normalizedMode, inspectOptions.PathPrefix, target.bucket, target.prefix, options.MaxKeys, options.IncludeHead, time.Since(started).Milliseconds(), err.Error())
		return nil, err
	}
	out := normalizeStorageBucketObjects(items, target)
	summary := summarizeProjectStorageObjects(out, target, normalizedMode)
	if normalizedMode != ProjectStorageInspectItems {
		out = []StorageBucketObject{}
	}
	log.Printf("INFO: syfon_project_storage_inspect_done organization=%s project=%s mode=%s path_prefix=%q bucket=%s prefix=%q max_keys=%d include_head=%t exists=%t object_count=%d returned_items=%d total_bytes=%d duration_ms=%d", organization, project, normalizedMode, inspectOptions.PathPrefix, target.bucket, target.prefix, options.MaxKeys, options.IncludeHead, summary.Exists, summary.ObjectCount, len(out), summary.TotalBytes, time.Since(started).Milliseconds())
	return &ProjectStorageInspectResult{Summary: summary, Items: out}, nil
}

func (target *resolvedStorageScopeTarget) withPathPrefix(pathPrefix string) *resolvedStorageScopeTarget {
	trimmed := strings.Trim(strings.TrimSpace(pathPrefix), "/")
	if target == nil || trimmed == "" {
		return target
	}
	copyTarget := *target
	if copyTarget.prefix == "" {
		copyTarget.prefix = trimmed
	} else {
		copyTarget.prefix = strings.Trim(strings.TrimSpace(copyTarget.prefix), "/") + "/" + trimmed
	}
	return &copyTarget
}

func normalizeProjectStorageInspectMode(mode ProjectStorageInspectMode) ProjectStorageInspectMode {
	switch mode {
	case ProjectStorageInspectExists, ProjectStorageInspectSummary:
		return mode
	default:
		return ProjectStorageInspectItems
	}
}

func normalizeStorageBucketObjects(items []StorageBucketObject, target *resolvedStorageScopeTarget) []StorageBucketObject {
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
	return out
}

func summarizeProjectStorageObjects(items []StorageBucketObject, target *resolvedStorageScopeTarget, mode ProjectStorageInspectMode) ProjectStorageSummary {
	summary := ProjectStorageSummary{
		Provider:    target.provider,
		Bucket:      target.bucket,
		Prefix:      strings.Trim(strings.TrimSpace(target.prefix), "/"),
		ObjectURL:   common.BucketToURL(target.bucket, strings.Trim(strings.TrimSpace(target.prefix), "/")),
		Exists:      len(items) > 0,
		ObjectCount: len(items),
		ComputedAt:  time.Now().UTC(),
		Mode:        mode,
	}
	for _, item := range items {
		summary.TotalBytes += item.SizeBytes
	}
	return summary
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

func (m *ObjectManager) ListValidateStorageObjects(ctx context.Context, items []StorageListValidationRequest) []StorageListValidationResult {
	started := time.Now()
	ctx = WithStorageInspectCache(ctx)
	if len(items) == 0 {
		log.Printf("INFO: syfon_bulk_list_validate_done items=0 duration_ms=0")
		return []StorageListValidationResult{}
	}
	results := make([]StorageListValidationResult, len(items))
	workers := len(items)
	if workers > maxStorageInspectWorkers {
		workers = maxStorageInspectWorkers
	}
	type workItem struct {
		index int
		req   StorageListValidationRequest
	}
	workCh := make(chan workItem)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for work := range workCh {
				results[work.index] = m.listValidateStorageObject(ctx, work.req)
			}
		}()
	}
	for index, item := range items {
		workCh <- workItem{index: index, req: item}
	}
	close(workCh)
	wg.Wait()
	statusCounts := map[StorageProbeStatus]int{}
	validationCounts := map[StorageValidationStatus]int{}
	for _, result := range results {
		statusCounts[result.Status]++
		validationCounts[result.ValidationStatus]++
	}
	log.Printf(
		"INFO: syfon_bulk_list_validate_done items=%d present=%d not_found=%d invalid=%d error=%d matched=%d mismatched=%d unverifiable=%d duration_ms=%d",
		len(items),
		statusCounts[StorageProbeStatusPresent],
		statusCounts[StorageProbeStatusNotFound],
		statusCounts[StorageProbeStatusInvalid],
		statusCounts[StorageProbeStatusError],
		validationCounts[StorageValidationMatched],
		validationCounts[StorageValidationMismatched],
		validationCounts[StorageValidationUnverifiable],
		time.Since(started).Milliseconds(),
	)
	return results
}

func (m *ObjectManager) listValidateStorageObject(ctx context.Context, req StorageListValidationRequest) StorageListValidationResult {
	result := StorageListValidationResult{
		ID:               strings.TrimSpace(req.ID),
		ObjectURL:        strings.TrimSpace(req.ObjectURL),
		Status:           StorageProbeStatusError,
		ValidationStatus: StorageValidationNotRequested,
	}
	target, ok, err := m.storageTargetFromURL(ctx, req.ObjectURL)
	if err != nil {
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}
	if !ok || target.provider != common.S3Provider {
		log.Printf("INFO: syfon_bulk_list_validate_resolve id=%s object_url=%q status=invalid provider=%s bucket=%s key=%q", result.ID, result.ObjectURL, target.provider, target.bucket, target.key)
		result.Status = StorageProbeStatusInvalid
		result.ErrorKind = string(StorageInspectInvalidInput)
		result.Error = "object_url must be a valid s3://bucket/key URL"
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}
	result.Provider = common.S3Provider
	result.Bucket = target.bucket
	result.Key = target.key
	result.Path = path.Base(target.key)
	log.Printf("INFO: syfon_bulk_list_validate_resolve id=%s object_url=%q bucket=%s key=%q expected_size_set=%t expected_name=%q", result.ID, result.ObjectURL, target.bucket, target.key, req.ExpectedSizeBytes != nil, strings.TrimSpace(req.ExpectedName))

	cred, err := m.credentialForBucket(ctx, target.bucket)
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_credential id=%s bucket=%s key=%q error=%q", result.ID, target.bucket, target.key, err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}
	log.Printf("INFO: syfon_bulk_list_validate_credential id=%s request_bucket=%s request_key=%q credential_id=%s credential_bucket=%s provider=%s", result.ID, target.bucket, target.key, credentialIDForCredential(*cred), strings.TrimSpace(cred.Bucket), strings.TrimSpace(cred.Provider))
	visible, err := m.ListVisibleBuckets(ctx)
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_visible id=%s bucket=%s key=%q error=%q", result.ID, target.bucket, target.key, err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}
	if !bucketVisibleToCaller(visible, target.bucket, credentialIDForCredential(*cred)) {
		err := &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: fmt.Sprintf("bucket %q is not visible to the caller", target.bucket)}
		log.Printf("INFO: syfon_bulk_list_validate_visible id=%s bucket=%s key=%q visible_count=%d error=%q", result.ID, target.bucket, target.key, len(visible), err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = err.Error()
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}

	listStart := time.Now()
	log.Printf("INFO: syfon_bulk_list_validate_list_start id=%s bucket=%s prefix=%q exact_prefix=true max_keys=1", result.ID, target.bucket, target.key)
	matches, err := m.listS3Prefix(ctx, *cred, target.bucket, target.key, StoragePrefixListOptions{ExactPrefix: true, MaxKeys: 1})
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_list_done id=%s bucket=%s prefix=%q duration_ms=%d error=%q", result.ID, target.bucket, target.key, time.Since(listStart).Milliseconds(), err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result
	}
	firstKey := ""
	if len(matches) > 0 {
		firstKey = strings.Trim(strings.TrimSpace(matches[0].Key), "/")
	}
	log.Printf("INFO: syfon_bulk_list_validate_list_done id=%s bucket=%s prefix=%q duration_ms=%d matches=%d first_key=%q", result.ID, target.bucket, target.key, time.Since(listStart).Milliseconds(), len(matches), firstKey)
	for _, item := range matches {
		key := strings.Trim(strings.TrimSpace(item.Key), "/")
		if key != target.key {
			log.Printf("INFO: syfon_bulk_list_validate_exact_miss id=%s requested_key=%q listed_key=%q", result.ID, target.key, key)
			continue
		}
		normalized := normalizeStorageBucketObjects([]StorageBucketObject{item}, &resolvedStorageScopeTarget{provider: common.S3Provider, bucket: target.bucket, prefix: target.key})
		if len(normalized) == 0 {
			break
		}
		return storageListValidationPresentResult(req, result, normalized[0])
	}
	result.Status = StorageProbeStatusNotFound
	result.ErrorKind = string(StorageInspectObjectNotFound)
	result.Error = fmt.Sprintf("object %q was not found", req.ObjectURL)
	result.ValidationStatus = storageListValidationStatusForError(req)
	return result
}

func storageListValidationPresentResult(req StorageListValidationRequest, result StorageListValidationResult, item StorageBucketObject) StorageListValidationResult {
	result.ObjectURL = item.ObjectURL
	result.Key = item.Key
	result.Path = item.Path
	result.Exists = true
	result.Status = StorageProbeStatusPresent
	result.Error = ""
	result.ErrorKind = ""
	result.SizeBytes = ptrInt64(item.SizeBytes)
	result.ETag = strings.TrimSpace(item.ETag)
	result.LastModTime = item.LastModTime
	result.ValidationStatus, result.SizeMatch, result.NameMatch, result.ValidationMismatches = validateStorageListResult(req, item)
	return result
}

func storageListValidationStatusForError(req StorageListValidationRequest) StorageValidationStatus {
	if req.ExpectedSizeBytes == nil && strings.TrimSpace(req.ExpectedName) == "" {
		return StorageValidationNotRequested
	}
	return StorageValidationUnverifiable
}

func validateStorageListResult(req StorageListValidationRequest, item StorageBucketObject) (StorageValidationStatus, *bool, *bool, []string) {
	if req.ExpectedSizeBytes == nil && strings.TrimSpace(req.ExpectedName) == "" {
		return StorageValidationNotRequested, nil, nil, nil
	}
	mismatches := make([]string, 0, 2)
	var sizeMatch *bool
	if req.ExpectedSizeBytes != nil {
		matched := item.SizeBytes == *req.ExpectedSizeBytes
		sizeMatch = &matched
		if !matched {
			mismatches = append(mismatches, "size_mismatch")
		}
	}
	var nameMatch *bool
	if expectedName := strings.TrimSpace(req.ExpectedName); expectedName != "" {
		matched := path.Base(item.Key) == expectedName
		nameMatch = &matched
		if !matched {
			mismatches = append(mismatches, "name_mismatch")
		}
	}
	if len(mismatches) > 0 {
		return StorageValidationMismatched, sizeMatch, nameMatch, mismatches
	}
	return StorageValidationMatched, sizeMatch, nameMatch, nil
}

func defaultS3PrefixLister(ctx context.Context, cred models.S3Credential, bucket string, prefix string, options StoragePrefixListOptions) ([]StorageBucketObject, error) {
	started := time.Now()
	client, err := s3ClientFromContext(ctx, cred)
	if err != nil {
		return nil, err
	}
	input := &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	if trimmedPrefix := strings.Trim(strings.TrimSpace(prefix), "/"); trimmedPrefix != "" {
		if options.ExactPrefix {
			input.Prefix = aws.String(trimmedPrefix)
		} else {
			input.Prefix = aws.String(trimmedPrefix + "/")
		}
	}
	if options.MaxKeys > 0 {
		input.MaxKeys = aws.Int32(options.MaxKeys)
	}
	requestPrefix := aws.ToString(input.Prefix)
	log.Printf("INFO: syfon_s3_prefix_list_start bucket=%s requested_prefix=%q input_prefix=%q exact_prefix=%t max_keys=%d include_head=%t", bucket, prefix, requestPrefix, options.ExactPrefix, options.MaxKeys, options.IncludeHead)
	paginator := awss3.NewListObjectsV2Paginator(client, input)
	out := make([]StorageBucketObject, 0)
	pageCount := 0
	firstKeys := make([]string, 0, 5)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			log.Printf("INFO: syfon_s3_prefix_list_done bucket=%s requested_prefix=%q input_prefix=%q exact_prefix=%t max_keys=%d include_head=%t pages=%d objects=%d duration_ms=%d error=%q", bucket, prefix, requestPrefix, options.ExactPrefix, options.MaxKeys, options.IncludeHead, pageCount, len(out), time.Since(started).Milliseconds(), err.Error())
			return nil, classifyS3ListError(bucket, prefix, err)
		}
		pageCount++
		for _, item := range page.Contents {
			key := strings.Trim(strings.TrimSpace(aws.ToString(item.Key)), "/")
			if key == "" {
				continue
			}
			if len(firstKeys) < cap(firstKeys) {
				firstKeys = append(firstKeys, key)
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
	log.Printf("INFO: syfon_s3_prefix_list_done bucket=%s requested_prefix=%q input_prefix=%q exact_prefix=%t max_keys=%d include_head=%t pages=%d objects=%d first_keys=%q duration_ms=%d", bucket, prefix, requestPrefix, options.ExactPrefix, options.MaxKeys, options.IncludeHead, pageCount, len(out), strings.Join(firstKeys, ","), time.Since(started).Milliseconds())
	if !options.IncludeHead || len(out) == 0 {
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
