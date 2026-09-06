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

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/requestmeta"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
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

const (
	storageListCoalesceThreshold   = 25
	storageListFallbackObjectLimit = 5000
	storageListSlowExactThreshold  = time.Second
	storageListSlowPrefixThreshold = 3 * time.Second
)

type ProjectStorageSummary struct {
	Provider          string
	Bucket            string
	Prefix            string
	ObjectURL         string
	Exists            bool
	ObjectCount       int
	TotalBytes        int64
	ComputedAt        time.Time
	Mode              ProjectStorageInspectMode
	InventoryComplete bool
	InventoryWarning  string
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

type storageListTargetWork struct {
	bucket         string
	key            string
	cred           buckets.Credential
	baseResult     StorageListValidationResult
	requestIndexes []int
}

type storageListRunStats struct {
	mu                   sync.Mutex
	inputItemCount       int
	distinctTargetCount  int
	duplicateCount       int
	bucketCount          int
	coalescedPrefixCount int
	exactListCallCount   int
	prefixListPageCount  int
	fallbackCount        int
}

func (s *storageListRunStats) incrementExactListCalls() {
	s.mu.Lock()
	s.exactListCallCount++
	s.mu.Unlock()
}

func (m *ObjectManager) inventoryStorageObjects(ctx context.Context, bucket, prefix string, options StoragePrefixListOptions) ([]StorageBucketObject, error) {
	if m.storageInventory == nil {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: "storage inventory is not configured"}
	}
	result, err := m.storageInventory.Inventory(ctx, storage.InventoryRequest{
		Target:      storage.PrefixTarget{Bucket: bucket, Prefix: prefix},
		IncludeHead: options.IncludeHead,
		ExactPrefix: options.ExactPrefix,
		MaxKeys:     options.MaxKeys,
	})
	items := make([]StorageBucketObject, 0, len(result.Items))
	for _, metadata := range result.Items {
		key := strings.Trim(strings.TrimSpace(metadata.Key), "/")
		if key == "" {
			continue
		}
		item := StorageBucketObject{
			ObjectURL:   address.BucketToURL(bucket, key),
			Provider:    strings.TrimSpace(metadata.Provider),
			Bucket:      strings.TrimSpace(metadata.Bucket),
			Key:         key,
			Path:        strings.TrimSpace(metadata.Path),
			SizeBytes:   metadata.SizeBytes,
			MetaSHA256:  strings.TrimSpace(metadata.MetaSHA256),
			ETag:        strings.TrimSpace(metadata.ETag),
			LastModTime: metadata.LastModified,
		}
		if item.Provider == "" {
			item.Provider = address.S3Provider
		}
		if item.Bucket == "" {
			item.Bucket = bucket
		}
		if item.Path == "" {
			item.Path = path.Base(key)
		}
		items = append(items, item)
	}
	if err != nil {
		return items, mapStorageOperationError(err, "inventory", bucket, prefix)
	}
	if !result.Complete {
		return items, &StorageInspectError{Kind: StorageInspectListingIncomplete, Message: fmt.Sprintf("provider returned an incomplete listing for s3://%s/%s", bucket, strings.Trim(strings.TrimSpace(prefix), "/"))}
	}
	return items, nil
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
	cred     buckets.Credential
}

type ProjectStorageDeleteResult struct {
	ObjectURL string
	Status    string
	Error     string
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
	items, listErr := m.inventoryStorageObjects(ctx, target.bucket, target.prefix, options)
	inventoryComplete := listErr == nil
	inventoryWarning := ""
	if listErr != nil {
		var inspectErr *StorageInspectError
		if len(items) == 0 || !errors.As(listErr, &inspectErr) || inspectErr.Kind != StorageInspectListingIncomplete {
			log.Printf("INFO: syfon_project_storage_inspect_done organization=%s project=%s mode=%s path_prefix=%q bucket=%s prefix=%q max_keys=%d include_head=%t duration_ms=%d error=%q", organization, project, normalizedMode, inspectOptions.PathPrefix, target.bucket, target.prefix, options.MaxKeys, options.IncludeHead, time.Since(started).Milliseconds(), listErr.Error())
			return nil, listErr
		}
		inventoryWarning = strings.TrimSpace(listErr.Error())
	}
	out := normalizeStorageBucketObjects(items, target)
	summary := summarizeProjectStorageObjects(out, target, normalizedMode)
	summary.InventoryComplete = inventoryComplete
	summary.InventoryWarning = inventoryWarning
	if normalizedMode != ProjectStorageInspectItems {
		out = []StorageBucketObject{}
	}
	log.Printf("INFO: syfon_project_storage_inspect_done organization=%s project=%s mode=%s path_prefix=%q bucket=%s prefix=%q max_keys=%d include_head=%t exists=%t object_count=%d returned_items=%d total_bytes=%d inventory_complete=%t inventory_warning=%q duration_ms=%d", organization, project, normalizedMode, inspectOptions.PathPrefix, target.bucket, target.prefix, options.MaxKeys, options.IncludeHead, summary.Exists, summary.ObjectCount, len(out), summary.TotalBytes, summary.InventoryComplete, summary.InventoryWarning, time.Since(started).Milliseconds())
	return &ProjectStorageInspectResult{Summary: summary, Items: out}, nil
}

func (m *ObjectManager) ResolveProjectStoragePathPrefix(ctx context.Context, organization, project, pathPrefix string) (string, error) {
	target, err := m.resolveProjectStorageScopeTarget(ctx, organization, project)
	if err != nil {
		return "", err
	}
	target = target.withPathPrefix(pathPrefix)
	if target == nil {
		return "", nil
	}
	return strings.Trim(strings.TrimSpace(target.prefix), "/"), nil
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
		entry.Provider = address.S3Provider
		entry.Bucket = target.bucket
		entry.Key = strings.Trim(strings.TrimSpace(entry.Key), "/")
		entry.ObjectURL = address.BucketToURL(target.bucket, entry.Key)
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
		Provider:          target.provider,
		Bucket:            target.bucket,
		Prefix:            strings.Trim(strings.TrimSpace(target.prefix), "/"),
		ObjectURL:         address.BucketToURL(target.bucket, strings.Trim(strings.TrimSpace(target.prefix), "/")),
		Exists:            len(items) > 0,
		ObjectCount:       len(items),
		ComputedAt:        time.Now().UTC(),
		Mode:              mode,
		InventoryComplete: true,
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
	if access.IsAuthzEnforced(ctx) && !access.HasMethodAccess(ctx, method, []string{resource}) {
		return nil, &access.AuthorizationError{Method: method, Resources: []string{resource}}
	}

	scopes := make([]buckets.Scope, 0, 2)
	if scope, found, err := m.bucketService.LookupBucketScope(ctx, organization, ""); err != nil {
		return nil, err
	} else if found {
		scopes = append(scopes, scope)
	}
	if project != "" {
		if scope, found, err := m.bucketService.LookupBucketScope(ctx, organization, project); err != nil {
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
	if address.NormalizeProvider(cred.Provider, address.S3Provider) != address.S3Provider {
		return nil, &StorageInspectError{Kind: StorageInspectUnsupported, Message: fmt.Sprintf("provider %q is not supported for scoped bucket listing", cred.Provider)}
	}
	return &resolvedStorageScopeTarget{
		provider: address.S3Provider,
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
	if address.NormalizeProvider(candidate.provider, address.S3Provider) != address.S3Provider {
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
	visible, visibleErr := m.listVisibleBucketsCached(ctx)
	results := make([]StorageListValidationResult, len(items))
	workByTarget := make(map[string]*storageListTargetWork)
	buckets := map[string]struct{}{}
	stats := storageListRunStats{inputItemCount: len(items)}
	for index, item := range items {
		result, targetWork, ok := m.resolveListValidationTarget(ctx, item, index, visible, visibleErr)
		if !ok {
			results[index] = result
			continue
		}
		targetKey := storageListTargetKey(targetWork.bucket, targetWork.key)
		if existing, found := workByTarget[targetKey]; found {
			existing.requestIndexes = append(existing.requestIndexes, index)
			stats.duplicateCount++
			continue
		}
		workByTarget[targetKey] = targetWork
		buckets[targetWork.bucket] = struct{}{}
	}
	stats.distinctTargetCount = len(workByTarget)
	stats.bucketCount = len(buckets)

	prefixGroups := groupStorageListTargetsByDirectory(workByTarget)
	coalescedPrefixGroups := make([][]*storageListTargetWork, 0, len(prefixGroups))
	for _, group := range prefixGroups {
		if len(group) < storageListCoalesceThreshold {
			continue
		}
		stats.coalescedPrefixCount++
		coalescedPrefixGroups = append(coalescedPrefixGroups, group)
	}
	log.Printf(
		"INFO: syfon_bulk_list_validate_start items=%d distinct_targets=%d duplicates=%d buckets=%d coalesced_prefixes=%d",
		stats.inputItemCount,
		stats.distinctTargetCount,
		stats.duplicateCount,
		stats.bucketCount,
		stats.coalescedPrefixCount,
	)

	outcomes := make(map[string]StorageListValidationResult, len(workByTarget))
	matchedItems := make(map[string]StorageBucketObject, len(workByTarget))
	unresolved := cloneStorageListWorkMap(workByTarget)

	for _, group := range coalescedPrefixGroups {
		m.runCoalescedStorageListGroup(ctx, group, outcomes, matchedItems, unresolved, &stats)
	}
	m.runExactStorageListTargets(ctx, unresolved, outcomes, matchedItems, &stats)

	for targetKey, work := range workByTarget {
		outcome, ok := outcomes[targetKey]
		if !ok {
			outcome = work.baseResult
			outcome.Status = StorageProbeStatusNotFound
			outcome.ErrorKind = string(StorageInspectObjectNotFound)
			outcome.Error = fmt.Sprintf("object %q was not found", work.baseResult.ObjectURL)
		}
		for _, index := range work.requestIndexes {
			req := items[index]
			if item, found := matchedItems[targetKey]; found {
				result := storageListValidationPresentResult(req, work.baseResult, item)
				result.ID = strings.TrimSpace(req.ID)
				result.ObjectURL = strings.TrimSpace(req.ObjectURL)
				results[index] = result
				continue
			}
			result := outcome
			result.ID = strings.TrimSpace(req.ID)
			result.ObjectURL = strings.TrimSpace(req.ObjectURL)
			result.ValidationStatus = storageListValidationStatusForError(req)
			results[index] = result
		}
	}

	statusCounts := map[StorageProbeStatus]int{}
	validationCounts := map[StorageValidationStatus]int{}
	for _, result := range results {
		statusCounts[result.Status]++
		validationCounts[result.ValidationStatus]++
	}
	log.Printf(
		"INFO: syfon_bulk_list_validate_done items=%d distinct_targets=%d duplicates=%d buckets=%d coalesced_prefixes=%d exact_list_calls=%d prefix_list_pages=%d fallbacks=%d visible_error=%t present=%d not_found=%d invalid=%d error=%d matched=%d mismatched=%d unverifiable=%d duration_ms=%d",
		stats.inputItemCount,
		stats.distinctTargetCount,
		stats.duplicateCount,
		stats.bucketCount,
		stats.coalescedPrefixCount,
		stats.exactListCallCount,
		stats.prefixListPageCount,
		stats.fallbackCount,
		visibleErr != nil,
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

func (m *ObjectManager) resolveListValidationTarget(ctx context.Context, req StorageListValidationRequest, index int, visible map[string]buckets.VisibleBucket, visibleErr error) (StorageListValidationResult, *storageListTargetWork, bool) {
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
		return result, nil, false
	}
	if !ok || target.provider != address.S3Provider {
		result.Status = StorageProbeStatusInvalid
		result.ErrorKind = string(StorageInspectInvalidInput)
		result.Error = "object_url must be a valid s3://bucket/key URL"
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result, nil, false
	}
	result.Provider = address.S3Provider
	result.Bucket = target.bucket
	result.Key = target.key
	result.Path = path.Base(target.key)

	cred, err := m.credentialForBucket(ctx, target.bucket)
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_credential id=%s bucket=%s key=%q error=%q", result.ID, target.bucket, target.key, err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result, nil, false
	}
	if visibleErr != nil {
		log.Printf("INFO: syfon_bulk_list_validate_visible id=%s bucket=%s key=%q error=%q", result.ID, target.bucket, target.key, visibleErr.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(visibleErr)
		result.Error = strings.TrimSpace(visibleErr.Error())
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result, nil, false
	}
	if !buckets.VisibleToCaller(visible, target.bucket, cred.CredentialID) {
		err := &StorageInspectError{Kind: StorageInspectPermissionDenied, Message: fmt.Sprintf("bucket %q is not visible to the caller", target.bucket)}
		log.Printf("INFO: syfon_bulk_list_validate_visible id=%s bucket=%s key=%q visible_count=%d error=%q", result.ID, target.bucket, target.key, len(visible), err.Error())
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = err.Error()
		result.ValidationStatus = storageListValidationStatusForError(req)
		return result, nil, false
	}
	return result, &storageListTargetWork{
		bucket:         target.bucket,
		key:            target.key,
		cred:           *cred,
		baseResult:     result,
		requestIndexes: []int{index},
	}, true
}

func groupStorageListTargetsByDirectory(workByTarget map[string]*storageListTargetWork) map[string][]*storageListTargetWork {
	groups := make(map[string][]*storageListTargetWork)
	for _, work := range workByTarget {
		dirPrefix := storageListDirectoryPrefix(work.key)
		groupKey := strings.TrimSpace(work.bucket) + "\x00" + dirPrefix
		groups[groupKey] = append(groups[groupKey], work)
	}
	return groups
}

func cloneStorageListWorkMap(input map[string]*storageListTargetWork) map[string]*storageListTargetWork {
	out := make(map[string]*storageListTargetWork, len(input))
	for key, work := range input {
		out[key] = work
	}
	return out
}

func storageListTargetKey(bucket string, key string) string {
	return strings.TrimSpace(bucket) + "\x00" + strings.Trim(strings.TrimSpace(key), "/")
}

func storageListDirectoryPrefix(key string) string {
	trimmed := strings.Trim(strings.TrimSpace(key), "/")
	if trimmed == "" {
		return ""
	}
	dir := strings.Trim(strings.TrimSpace(path.Dir(trimmed)), "/")
	if dir == "." || dir == "" {
		return ""
	}
	return dir + "/"
}

func (m *ObjectManager) runCoalescedStorageListGroup(ctx context.Context, group []*storageListTargetWork, outcomes map[string]StorageListValidationResult, matchedItems map[string]StorageBucketObject, unresolved map[string]*storageListTargetWork, stats *storageListRunStats) {
	if len(group) == 0 {
		return
	}
	requestedKeys := make(map[string]*storageListTargetWork, len(group))
	for _, work := range group {
		requestedKeys[storageListTargetKey(work.bucket, work.key)] = work
	}
	dirPrefix := storageListDirectoryPrefix(group[0].key)
	started := time.Now()
	listed, err := m.inventoryStorageObjects(ctx, group[0].bucket, dirPrefix, StoragePrefixListOptions{ExactPrefix: true})
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_prefix bucket=%s prefix=%q targets=%d duration_ms=%d error=%q", group[0].bucket, dirPrefix, len(group), time.Since(started).Milliseconds(), err.Error())
		stats.fallbackCount += len(group)
		return
	}
	pageEstimate := 1
	if len(listed) > 1000 {
		pageEstimate = (len(listed) + 999) / 1000
	}
	stats.prefixListPageCount += pageEstimate
	found := 0
	for _, item := range listed {
		key := storageListTargetKey(group[0].bucket, item.Key)
		work, ok := requestedKeys[key]
		if !ok {
			continue
		}
		normalized := normalizeStorageBucketObjects([]StorageBucketObject{item}, &resolvedStorageScopeTarget{provider: address.S3Provider, bucket: work.bucket, prefix: work.key})
		if len(normalized) == 0 {
			continue
		}
		matchedItems[key] = normalized[0]
		result := work.baseResult
		result.Exists = true
		result.Status = StorageProbeStatusPresent
		result.Error = ""
		result.ErrorKind = ""
		outcomes[key] = result
		delete(unresolved, key)
		found++
	}
	if len(listed) > storageListFallbackObjectLimit && found < len(group) {
		stats.fallbackCount += len(group) - found
	}
	if duration := time.Since(started); duration > storageListSlowPrefixThreshold {
		log.Printf("INFO: syfon_bulk_list_validate_prefix bucket=%s prefix=%q targets=%d listed_objects=%d matched_targets=%d duration_ms=%d", group[0].bucket, dirPrefix, len(group), len(listed), found, duration.Milliseconds())
	}
}

func (m *ObjectManager) runExactStorageListTargets(ctx context.Context, unresolved map[string]*storageListTargetWork, outcomes map[string]StorageListValidationResult, matchedItems map[string]StorageBucketObject, stats *storageListRunStats) {
	workers := len(unresolved)
	if workers > maxStorageInspectWorkers {
		workers = maxStorageInspectWorkers
	}
	if workers == 0 {
		return
	}
	keys := make([]string, 0, len(unresolved))
	for key := range unresolved {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	workCh := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for targetKey := range workCh {
				work := unresolved[targetKey]
				outcome, item, matched := m.runExactStorageListTarget(ctx, work, stats)
				mu.Lock()
				outcomes[targetKey] = outcome
				if matched {
					matchedItems[targetKey] = item
				}
				mu.Unlock()
			}
		}()
	}
	for _, key := range keys {
		workCh <- key
	}
	close(workCh)
	wg.Wait()
}

func (m *ObjectManager) runExactStorageListTarget(ctx context.Context, work *storageListTargetWork, stats *storageListRunStats) (StorageListValidationResult, StorageBucketObject, bool) {
	started := time.Now()
	matches, err := m.inventoryStorageObjects(ctx, work.bucket, work.key, StoragePrefixListOptions{ExactPrefix: true, MaxKeys: 1})
	if err != nil {
		log.Printf("INFO: syfon_bulk_list_validate_exact request_id=%s bucket=%s key=%q duration_ms=%d error=%q", requestmeta.GetRequestID(ctx), work.bucket, work.key, time.Since(started).Milliseconds(), err.Error())
		result := work.baseResult
		result.Status, result.ErrorKind = classifyStorageProbeError(err)
		result.Error = strings.TrimSpace(err.Error())
		stats.incrementExactListCalls()
		return result, StorageBucketObject{}, false
	}
	stats.incrementExactListCalls()
	for _, item := range matches {
		key := strings.Trim(strings.TrimSpace(item.Key), "/")
		if key != work.key {
			continue
		}
		normalized := normalizeStorageBucketObjects([]StorageBucketObject{item}, &resolvedStorageScopeTarget{provider: address.S3Provider, bucket: work.bucket, prefix: work.key})
		if len(normalized) == 0 {
			break
		}
		if duration := time.Since(started); duration > storageListSlowExactThreshold {
			log.Printf("INFO: syfon_bulk_list_validate_exact request_id=%s bucket=%s key=%q duration_ms=%d status=present", requestmeta.GetRequestID(ctx), work.bucket, work.key, duration.Milliseconds())
		}
		result := work.baseResult
		result.Exists = true
		result.Status = StorageProbeStatusPresent
		result.Error = ""
		result.ErrorKind = ""
		return result, normalized[0], true
	}
	if duration := time.Since(started); duration > storageListSlowExactThreshold {
		log.Printf("INFO: syfon_bulk_list_validate_exact request_id=%s bucket=%s key=%q duration_ms=%d status=not_found", requestmeta.GetRequestID(ctx), work.bucket, work.key, duration.Milliseconds())
	}
	result := work.baseResult
	result.Status = StorageProbeStatusNotFound
	result.ErrorKind = string(StorageInspectObjectNotFound)
	result.Error = fmt.Sprintf("object %q was not found", work.baseResult.ObjectURL)
	return result, StorageBucketObject{}, false
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
