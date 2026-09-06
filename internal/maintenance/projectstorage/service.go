package projectstorage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
)

const (
	readMethod              = "read"
	deleteMethod            = "delete"
	maxProbeWorkers         = 8
	listCoalesceThreshold   = 25
	listFallbackObjectLimit = 5000
)

type Service struct {
	scopes      ScopeReader
	credentials CredentialReader
	visibility  VisibilityReader
	inventory   InventoryPort
	probe       ProbePort
	delete      DeletePort
	physical    PhysicalScopeReader
	cleanup     CleanupDependencies
}

func NewService(scopes ScopeReader, credentials CredentialReader, visibility VisibilityReader, inventory InventoryPort, probe ProbePort, deletePort DeletePort, physical PhysicalScopeReader, cleanup ...CleanupDependencies) *Service {
	service := &Service{
		scopes:      scopes,
		credentials: credentials,
		visibility:  visibility,
		inventory:   inventory,
		probe:       probe,
		delete:      deletePort,
		physical:    physical,
	}
	if len(cleanup) > 0 {
		service.cleanup = cleanup[0]
	}
	return service
}

// InspectProject resolves a project scope, inventories it, and computes the
// summary. An incomplete listing remains usable when the provider returned
// partial items; the warning is retained in the result instead of being
// mistaken for a complete inventory.
func (s *Service) InspectProject(ctx context.Context, organization, project string, options InspectionOptions) (*InspectionResult, error) {
	return s.InspectProjectStorage(ctx, organization, project, options)
}

// InspectProjectStorage inventories the S3 target selected by the project's
// configured scope. InspectProject is retained as a concise local spelling;
// this name is the maintenance service's composition-facing contract.
func (s *Service) InspectProjectStorage(ctx context.Context, organization, project string, options InspectionOptions) (*InspectionResult, error) {
	ctx = withRequestCache(ctx)
	target, err := s.resolveScope(ctx, organization, project, readMethod)
	if err != nil {
		return nil, err
	}
	target = target.withPathPrefix(options.PathPrefix)
	mode := normalizeMode(options.Mode)
	listOptions := InventoryOptions{IncludeHead: options.IncludeHead}
	if mode == ModeExists {
		listOptions.MaxKeys = 1
	}
	items, listErr := s.inventoryObjects(ctx, target.Bucket, target.Prefix, listOptions)
	complete := listErr == nil
	warning := ""
	if listErr != nil {
		var inspectErr *Error
		if len(items) == 0 || !errors.As(listErr, &inspectErr) || inspectErr.Kind != ErrorListingIncomplete {
			return nil, listErr
		}
		warning = strings.TrimSpace(listErr.Error())
	}
	normalized := normalizeObjects(items, target)
	summary := summarize(normalized, target, mode)
	summary.InventoryComplete = complete
	summary.InventoryWarning = warning
	if mode != ModeItems {
		normalized = []StorageObject{}
	}
	return &InspectionResult{Summary: summary, Items: normalized}, nil
}

func (s *Service) ListObjects(ctx context.Context, organization, project string, includeHead bool) ([]StorageObject, error) {
	result, err := s.InspectProject(ctx, organization, project, InspectionOptions{Mode: ModeItems, IncludeHead: includeHead})
	if err != nil {
		return nil, err
	}
	return result.Items, nil
}

func (s *Service) ResolvePathPrefix(ctx context.Context, organization, project, requestPrefix string) (string, error) {
	target, err := s.resolveScope(withRequestCache(ctx), organization, project, readMethod)
	if err != nil {
		return "", err
	}
	return strings.Trim(strings.TrimSpace(target.withPathPrefix(requestPrefix).Prefix), "/"), nil
}

// AuditProjectRecords projects physical records for the project-record
// inspection route. Unlike prepared inventory reads, it intentionally keeps
// same-checksum physical duplicates visible and only emits records carrying a
// primary SHA-256 checksum.
func (s *Service) AuditProjectRecords(ctx context.Context, organization, project, requestPrefix string) ([]ProjectRecordAudit, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" {
		return nil, &Error{Kind: ErrorInvalidInput, Message: "organization and project are required"}
	}
	if s.physical == nil {
		return nil, &Error{Kind: ErrorUnsupported, Message: "physical scope reader is not configured"}
	}
	records, err := s.physical.ListPhysicalObjectsByScope(ctx, organization, project, readMethod)
	if err != nil {
		return nil, err
	}
	prefixes := make([]string, 0, 2)
	if prefix := strings.Trim(strings.TrimSpace(requestPrefix), "/"); prefix != "" {
		prefixes = append(prefixes, prefix)
		if resolved, resolveErr := s.ResolvePathPrefix(ctx, organization, project, prefix); resolveErr == nil && resolved != "" && !strings.EqualFold(resolved, prefix) {
			prefixes = append(prefixes, resolved)
		}
	}
	result := make([]ProjectRecordAudit, 0, len(records))
	for _, record := range records {
		item, ok := projectRecordFromRecord(record, organization, project)
		if !ok || (len(prefixes) > 0 && !projectRecordMatchesPrefix(item, prefixes...)) {
			continue
		}
		result = append(result, item)
	}
	return result, nil
}

func projectRecordFromRecord(record objects.Record, organization, project string) (ProjectRecordAudit, bool) {
	checksum, ok := objects.CanonicalSHA256(record.Checksums)
	if !ok || strings.TrimSpace(checksum) == "" {
		return ProjectRecordAudit{}, false
	}
	item := ProjectRecordAudit{ObjectID: string(record.Id), Checksum: checksum, Organization: organization, Project: project, Size: record.Size, CreatedTime: record.CreatedTime}
	if record.Name != nil {
		item.Name = strings.TrimSpace(*record.Name)
	}
	if record.UpdatedTime != nil {
		updated := *record.UpdatedTime
		item.UpdatedTime = &updated
	}
	if record.AccessMethods != nil {
		item.AccessMethods = make([]ProjectAccessMethod, 0, len(*record.AccessMethods))
		for _, method := range *record.AccessMethods {
			access := ProjectAccessMethod{Type: strings.TrimSpace(method.Type)}
			if method.AccessId != nil {
				access.AccessID = strings.TrimSpace(*method.AccessId)
			}
			if method.AccessUrl != nil {
				access.URL = strings.TrimSpace(method.AccessUrl.Url)
				if access.URL != "" {
					item.AccessURLs = append(item.AccessURLs, access.URL)
				}
				if method.AccessUrl.Headers != nil {
					access.Headers = append([]string(nil), (*method.AccessUrl.Headers)...)
				}
			}
			item.AccessMethods = append(item.AccessMethods, access)
		}
	}
	return item, true
}

func projectRecordMatchesPrefix(record ProjectRecordAudit, prefixes ...string) bool {
	for _, rawPrefix := range prefixes {
		prefix := strings.Trim(strings.TrimSpace(rawPrefix), "/")
		if prefix == "" {
			return true
		}
		for _, rawURL := range record.AccessURLs {
			_, key, ok := address.ParseS3URL(rawURL)
			if ok && storageKeyWithinPrefix(key, prefix) {
				return true
			}
		}
		for _, method := range record.AccessMethods {
			_, key, ok := address.ParseS3URL(method.URL)
			if ok && storageKeyWithinPrefix(key, prefix) {
				return true
			}
		}
	}
	return false
}

func storageKeyWithinPrefix(key, prefix string) bool {
	key = strings.Trim(strings.TrimSpace(key), "/")
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}

func (s *Service) inventoryObjects(ctx context.Context, bucket, prefix string, options InventoryOptions) ([]StorageObject, error) {
	if s.inventory == nil {
		return nil, &Error{Kind: ErrorUnsupported, Message: "storage inventory is not configured"}
	}
	result, err := s.inventory.Inventory(ctx, storage.InventoryRequest{
		Target:      storage.PrefixTarget{Bucket: bucket, Prefix: prefix},
		IncludeHead: options.IncludeHead,
		ExactPrefix: options.ExactPrefix,
		MaxKeys:     options.MaxKeys,
	})
	items := make([]StorageObject, 0, len(result.Items))
	for _, metadata := range result.Items {
		key := strings.Trim(strings.TrimSpace(metadata.Key), "/")
		if key == "" {
			continue
		}
		item := StorageObject{
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
		return items, mapStorageError(err, "inventory", bucket, prefix)
	}
	if !result.Complete {
		return items, &Error{Kind: ErrorListingIncomplete, Message: fmt.Sprintf("provider returned an incomplete listing for s3://%s/%s", bucket, strings.Trim(strings.TrimSpace(prefix), "/"))}
	}
	return items, nil
}

func normalizeMode(mode InspectionMode) InspectionMode {
	switch mode {
	case ModeExists, ModeSummary:
		return mode
	default:
		return ModeItems
	}
}

type scopeTarget struct {
	Provider   string
	Bucket     string
	Prefix     string
	Credential buckets.Credential
}

func (target scopeTarget) withPathPrefix(requestPrefix string) scopeTarget {
	trimmed := strings.Trim(strings.TrimSpace(requestPrefix), "/")
	if trimmed == "" {
		return target
	}
	if target.Prefix == "" {
		target.Prefix = trimmed
	} else {
		target.Prefix = strings.Trim(strings.TrimSpace(target.Prefix), "/") + "/" + trimmed
	}
	return target
}

func (s *Service) resolveScope(ctx context.Context, organization, project, method string) (scopeTarget, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		return scopeTarget{}, &Error{Kind: ErrorInvalidInput, Message: "organization is required"}
	}
	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return scopeTarget{}, &Error{Kind: ErrorInvalidInput, Message: err.Error()}
	}
	if access.IsAuthzEnforced(ctx) && !access.HasMethodAccess(ctx, method, []string{resource}) {
		return scopeTarget{}, &access.AuthorizationError{Method: method, Resources: []string{resource}}
	}
	if s.scopes == nil {
		return scopeTarget{}, &Error{Kind: ErrorUnsupported, Message: "bucket scope reader is not configured"}
	}
	scopes := make([]buckets.Scope, 0, 2)
	if scope, found, lookupErr := s.scopes.LookupBucketScope(ctx, organization, ""); lookupErr != nil {
		return scopeTarget{}, lookupErr
	} else if found {
		scopes = append(scopes, scope)
	}
	if project != "" {
		if scope, found, lookupErr := s.scopes.LookupBucketScope(ctx, organization, project); lookupErr != nil {
			return scopeTarget{}, lookupErr
		} else if found {
			scopes = append(scopes, scope)
		}
	}
	if len(scopes) == 0 {
		if project != "" {
			return scopeTarget{}, &Error{Kind: ErrorScopeNotFound, Message: fmt.Sprintf("no bucket scope configured for organization %q project %q", organization, project)}
		}
		return scopeTarget{}, &Error{Kind: ErrorScopeNotFound, Message: fmt.Sprintf("no bucket scope configured for organization %q", organization)}
	}
	bucket := ""
	for _, scope := range scopes {
		if candidate := strings.TrimSpace(scope.Bucket); candidate != "" {
			bucket = candidate
		}
	}
	if bucket == "" {
		return scopeTarget{}, &Error{Kind: ErrorInvalidInput, Message: fmt.Sprintf("unable to resolve scoped storage bucket for organization %q project %q", organization, project)}
	}
	credential, err := s.credentialForBucket(ctx, bucket)
	if err != nil {
		return scopeTarget{}, err
	}
	if address.NormalizeProvider(credential.Provider, address.S3Provider) != address.S3Provider {
		return scopeTarget{}, &Error{Kind: ErrorUnsupported, Message: fmt.Sprintf("provider %q is not supported for scoped bucket listing", credential.Provider)}
	}
	return scopeTarget{
		Provider:   address.S3Provider,
		Bucket:     bucket,
		Prefix:     strings.Join(normalizedPrefixes(scopes), "/"),
		Credential: *credential,
	}, nil
}

func normalizedPrefixes(scopes []buckets.Scope) []string {
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

func normalizeObjects(items []StorageObject, target scopeTarget) []StorageObject {
	out := make([]StorageObject, 0, len(items))
	for _, item := range items {
		item.Provider = address.S3Provider
		item.Bucket = target.Bucket
		item.Key = strings.Trim(strings.TrimSpace(item.Key), "/")
		item.ObjectURL = address.BucketToURL(target.Bucket, item.Key)
		if strings.TrimSpace(item.Path) == "" {
			item.Path = path.Base(item.Key)
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func summarize(items []StorageObject, target scopeTarget, mode InspectionMode) Summary {
	result := Summary{
		Provider:          target.Provider,
		Bucket:            target.Bucket,
		Prefix:            strings.Trim(strings.TrimSpace(target.Prefix), "/"),
		ObjectURL:         address.BucketToURL(target.Bucket, strings.Trim(strings.TrimSpace(target.Prefix), "/")),
		Exists:            len(items) > 0,
		ObjectCount:       len(items),
		ComputedAt:        time.Now().UTC(),
		Mode:              mode,
		InventoryComplete: true,
	}
	for _, item := range items {
		result.TotalBytes += item.SizeBytes
	}
	return result
}

func mapStorageError(err error, capability, bucket, key string) error {
	if err == nil {
		return nil
	}
	var operation *storage.OperationError
	if !errors.As(err, &operation) {
		return err
	}
	kind := ErrorBucketUnavailable
	switch operation.Kind {
	case storage.ErrorInvalid:
		kind = ErrorInvalidInput
	case storage.ErrorNotFound:
		if strings.TrimSpace(operation.Provider) == "" {
			kind = ErrorCredentialMissing
		} else {
			kind = ErrorObjectNotFound
		}
	case storage.ErrorForbidden:
		kind = ErrorPermissionDenied
	case storage.ErrorUnavailable:
		kind = ErrorBucketUnavailable
	case storage.ErrorIncomplete:
		kind = ErrorListingIncomplete
	case storage.ErrorUnsupported:
		kind = ErrorUnsupported
	case storage.ErrorProvider:
		return err
	}
	message := operation.Error()
	if strings.TrimSpace(message) == "" {
		message = fmt.Sprintf("storage %s failed for %s/%s", capability, bucket, key)
	}
	return &Error{Kind: kind, Message: message}
}

func (s *Service) credentialForBucket(ctx context.Context, bucket string) (*buckets.Credential, error) {
	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return nil, &Error{Kind: ErrorInvalidInput, Message: "bucket is required"}
	}
	if cache := cacheFromContext(ctx); cache != nil {
		if credential, err, ok := cache.credential(bucket); ok {
			return credential, err
		}
	}
	if s.credentials == nil {
		err := &Error{Kind: ErrorUnsupported, Message: "bucket credential reader is not configured"}
		cacheCredential(ctx, bucket, nil, err)
		return nil, err
	}
	if credential, err := s.credentials.GetS3Credential(ctx, bucket); err == nil && credential != nil {
		copy := *credential
		cacheCredential(ctx, bucket, &copy, nil)
		return &copy, nil
	}
	credentials, err := s.credentials.ListS3Credentials(ctx)
	if err != nil {
		cacheCredential(ctx, bucket, nil, err)
		return nil, err
	}
	for _, credential := range credentials {
		if strings.EqualFold(strings.TrimSpace(credential.Bucket), bucket) || strings.EqualFold(strings.TrimSpace(credential.CredentialID), bucket) {
			copy := credential
			cacheCredential(ctx, bucket, &copy, nil)
			return &copy, nil
		}
	}
	err = &Error{Kind: ErrorCredentialMissing, Message: fmt.Sprintf("no stored bucket credential found for bucket %q", bucket)}
	cacheCredential(ctx, bucket, nil, err)
	return nil, err
}

func (s *Service) visibleBuckets(ctx context.Context) (map[string]buckets.VisibleBucket, error) {
	if cache := cacheFromContext(ctx); cache != nil {
		if visible, err, ok := cache.visible(); ok {
			return visible, err
		}
	}
	if s.visibility == nil {
		err := &Error{Kind: ErrorUnsupported, Message: "bucket visibility reader is not configured"}
		cacheVisible(ctx, nil, err)
		return nil, err
	}
	visible, err := s.visibility.ListVisibleBuckets(ctx)
	cacheVisible(ctx, visible, err)
	return cloneVisible(visible), err
}

func (s *Service) ProbeObject(ctx context.Context, request InspectRequest) (*ObjectMetadata, error) {
	ctx = withRequestCache(ctx)
	if strings.TrimSpace(request.ObjectURL) != "" {
		return s.inspectRaw(ctx, request)
	}
	return s.inspectScoped(ctx, request)
}

func (s *Service) ProbeObjects(ctx context.Context, requests []InspectRequest) []ProbeResult {
	ctx = withRequestCache(ctx)
	if len(requests) == 0 {
		return []ProbeResult{}
	}
	results := make([]ProbeResult, len(requests))
	workers := len(requests)
	if workers > maxProbeWorkers {
		workers = maxProbeWorkers
	}
	workCh := make(chan int)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range workCh {
				results[index] = s.probeOne(ctx, requests[index])
			}
		}()
	}
	for index := range requests {
		workCh <- index
	}
	close(workCh)
	wg.Wait()
	return results
}

func (s *Service) probeOne(ctx context.Context, request InspectRequest) ProbeResult {
	key := probeCacheKey(request)
	if cache := cacheFromContext(ctx); cache != nil {
		if result, ok := cache.probe(key); ok {
			result.ID = strings.TrimSpace(request.ID)
			if result.ObjectURL == "" {
				result.ObjectURL = strings.TrimSpace(request.ObjectURL)
			}
			result.ValidationStatus, result.SizeMatch, result.SHA256Match, result.ValidationMismatches = validateProbe(request, &ObjectMetadata{
				SizeBytes:  valueInt64(result.SizeBytes),
				MetaSHA256: result.MetaSHA256,
			})
			return result
		}
	}
	result := ProbeResult{ID: strings.TrimSpace(request.ID), ObjectURL: strings.TrimSpace(request.ObjectURL), Status: ProbeError, ValidationStatus: validationStatusForError(request)}
	metadata, err := s.ProbeObject(ctx, request)
	if err != nil {
		result.Status, result.ErrorKind = classifyError(err)
		result.Error = strings.TrimSpace(err.Error())
		if cache := cacheFromContext(ctx); cache != nil {
			cache.setProbe(key, result)
		}
		return result
	}
	result.ObjectURL = metadata.ObjectURL
	result.Provider = metadata.Provider
	result.Bucket = metadata.Bucket
	result.Key = metadata.Key
	result.Path = metadata.Path
	result.Exists = true
	result.Status = ProbePresent
	result.SizeBytes = int64Pointer(metadata.SizeBytes)
	result.MetaSHA256 = metadata.MetaSHA256
	result.ETag = metadata.ETag
	result.LastModTime = metadata.LastModTime
	result.ValidationStatus, result.SizeMatch, result.SHA256Match, result.ValidationMismatches = validateProbe(request, metadata)
	if cache := cacheFromContext(ctx); cache != nil {
		cache.setProbe(key, result)
	}
	return result
}

func probeCacheKey(request InspectRequest) string {
	key := strings.TrimSpace(request.ObjectURL) + "|" + strings.TrimSpace(request.Organization) + "|" + strings.TrimSpace(request.Project) + "|" + strings.TrimSpace(request.Key) + "|" + strings.TrimSpace(request.Scheme)
	if request.ExpectedSizeBytes != nil {
		key += fmt.Sprintf("|%d", *request.ExpectedSizeBytes)
	} else {
		key += "|"
	}
	return key + "|" + strings.ToLower(strings.TrimSpace(strings.TrimPrefix(request.ExpectedSHA256, "sha256:")))
}

func valueInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func (s *Service) inspectRaw(ctx context.Context, request InspectRequest) (*ObjectMetadata, error) {
	bucket, key, ok := address.ParseS3URL(strings.TrimSpace(request.ObjectURL))
	if !ok {
		return nil, &Error{Kind: ErrorInvalidInput, Message: "object_url must be a valid s3://bucket/key URL"}
	}
	credential, err := s.credentialForBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	visible, err := s.visibleBuckets(ctx)
	if err != nil {
		return nil, err
	}
	if !buckets.VisibleToCaller(visible, bucket, credential.CredentialID) {
		return nil, &Error{Kind: ErrorPermissionDenied, Message: fmt.Sprintf("bucket %q is not visible to the caller", bucket)}
	}
	if address.NormalizeProvider(credential.Provider, address.S3Provider) != address.S3Provider {
		return nil, &Error{Kind: ErrorUnsupported, Message: fmt.Sprintf("provider %q is not supported for server-backed add-url inspection", credential.Provider)}
	}
	metadata, err := s.probeStorage(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	metadata.ObjectURL = address.BucketToURL(bucket, key)
	metadata.Provider = address.S3Provider
	metadata.Bucket = bucket
	metadata.Key = key
	if metadata.Path == "" {
		metadata.Path = path.Base(key)
	}
	return metadata, nil
}

func (s *Service) inspectScoped(ctx context.Context, request InspectRequest) (*ObjectMetadata, error) {
	organization := strings.TrimSpace(request.Organization)
	project := strings.TrimSpace(request.Project)
	key := strings.Trim(strings.TrimSpace(request.Key), "/")
	if organization == "" {
		return nil, &Error{Kind: ErrorInvalidInput, Message: "organization is required for scoped object inspection"}
	}
	if key == "" {
		return nil, &Error{Kind: ErrorInvalidInput, Message: "key is required for scoped object inspection"}
	}
	scheme := strings.ToLower(strings.TrimSpace(request.Scheme))
	if scheme == "" {
		scheme = address.S3Provider
	}
	if address.ProviderFromScheme(scheme) != address.S3Provider && scheme != address.S3Provider {
		return nil, &Error{Kind: ErrorUnsupported, Message: fmt.Sprintf("provider scheme %q is not supported for server-backed add-url inspection", scheme)}
	}
	target, err := s.resolveScope(ctx, organization, project, readMethod)
	if err != nil {
		return nil, err
	}
	if target.Prefix != "" {
		key = path.Join(target.Prefix, key)
	}
	metadata, err := s.probeStorage(ctx, target.Bucket, key)
	if err != nil {
		return nil, err
	}
	metadata.ObjectURL = address.BucketToURL(target.Bucket, key)
	metadata.Provider = address.S3Provider
	metadata.Bucket = target.Bucket
	metadata.Key = key
	if metadata.Path == "" {
		metadata.Path = path.Base(key)
	}
	return metadata, nil
}

func (s *Service) probeStorage(ctx context.Context, bucket, key string) (*ObjectMetadata, error) {
	if s.probe == nil {
		return nil, &Error{Kind: ErrorUnsupported, Message: "storage probe is not configured"}
	}
	results := s.probe.Probe(ctx, []storage.ProbeTarget{{ID: "object", Target: storage.ObjectTarget{Bucket: bucket, Key: key}}})
	if len(results) == 0 {
		return nil, &Error{Kind: ErrorBucketUnavailable, Message: "storage probe returned no result"}
	}
	result := results[0]
	if result.Err != nil {
		return nil, mapStorageError(result.Err, "probe", bucket, key)
	}
	metadata := result.Metadata
	return &ObjectMetadata{Provider: strings.TrimSpace(metadata.Provider), Bucket: strings.TrimSpace(metadata.Bucket), Key: strings.TrimSpace(metadata.Key), Path: strings.TrimSpace(metadata.Path), SizeBytes: metadata.SizeBytes, MetaSHA256: strings.TrimSpace(metadata.MetaSHA256), ETag: strings.TrimSpace(metadata.ETag), LastModTime: metadata.LastModified}, nil
}

func classifyError(err error) (ProbeStatus, string) {
	var inspectErr *Error
	if errors.As(err, &inspectErr) {
		switch inspectErr.Kind {
		case ErrorObjectNotFound:
			return ProbeNotFound, string(inspectErr.Kind)
		case ErrorPermissionDenied, ErrorBucketUnavailable:
			return ProbeForbidden, string(inspectErr.Kind)
		case ErrorInvalidInput, ErrorScopeNotFound, ErrorCredentialMissing:
			return ProbeInvalid, string(inspectErr.Kind)
		case ErrorUnsupported:
			return ProbeUnsupported, string(inspectErr.Kind)
		}
		return ProbeError, string(inspectErr.Kind)
	}
	return ProbeError, "error"
}

func validationStatusForError(request InspectRequest) ValidationStatus {
	if request.ExpectedSizeBytes == nil && strings.TrimSpace(request.ExpectedSHA256) == "" {
		return ValidationNotRequested
	}
	return ValidationUnverifiable
}

func validateProbe(request InspectRequest, metadata *ObjectMetadata) (ValidationStatus, *bool, *bool, []string) {
	if request.ExpectedSizeBytes == nil && strings.TrimSpace(request.ExpectedSHA256) == "" {
		return ValidationNotRequested, nil, nil, nil
	}
	mismatches := make([]string, 0, 2)
	var sizeMatch *bool
	if request.ExpectedSizeBytes != nil {
		matched := metadata.SizeBytes == *request.ExpectedSizeBytes
		sizeMatch = &matched
		if !matched {
			mismatches = append(mismatches, "size_mismatch")
		}
	}
	var shaMatch *bool
	expectedSHA := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(request.ExpectedSHA256, "sha256:")))
	if expectedSHA != "" {
		if strings.TrimSpace(metadata.MetaSHA256) == "" {
			return ValidationUnverifiable, sizeMatch, nil, append(mismatches, "missing_remote_sha256")
		}
		matched := strings.EqualFold(strings.TrimSpace(metadata.MetaSHA256), expectedSHA)
		shaMatch = &matched
		if !matched {
			mismatches = append(mismatches, "sha256_mismatch")
		}
	}
	if len(mismatches) > 0 {
		return ValidationMismatched, sizeMatch, shaMatch, mismatches
	}
	return ValidationMatched, sizeMatch, shaMatch, nil
}

func int64Pointer(value int64) *int64 {
	copy := value
	return &copy
}

// DeleteProjectObjects authorizes each URL against the resolved S3 scope,
// deduplicates in first-seen order, and dispatches exact physical URLs one at
// a time. This preserves per-item provider failures and retry ordering.
func (s *Service) DeleteProjectObjects(ctx context.Context, organization, project string, objectURLs []string) []DeleteResult {
	ctx = withRequestCache(ctx)
	unique := uniqueURLs(objectURLs)
	if len(unique) == 0 {
		return []DeleteResult{}
	}
	target, err := s.resolveScope(ctx, organization, project, deleteMethod)
	if err != nil {
		results := make([]DeleteResult, 0, len(unique))
		for _, objectURL := range unique {
			results = append(results, DeleteResult{ObjectURL: objectURL, Status: "error", Error: err.Error()})
		}
		return results
	}
	results := make([]DeleteResult, 0, len(unique))
	for _, objectURL := range unique {
		result := DeleteResult{ObjectURL: objectURL, Status: "deleted"}
		candidate, parseStatus, parseErr := parseDeleteURL(ctx, s, objectURL)
		switch {
		case parseErr != nil:
			result.Status = "error"
			result.Error = parseErr.Error()
		case parseStatus == "invalid":
			result.Status = "invalid"
			result.Error = "object_url must resolve to a deletable storage target"
		case !targetAllowed(candidate, target):
			result.Status = "forbidden"
			result.Error = fmt.Sprintf("object_url %q is outside configured project bucket scope", objectURL)
		default:
			if s.delete == nil {
				result.Status = "error"
				result.Error = "storage deletion is not configured"
			} else if deleteErr := s.delete.DeleteExact(ctx, []storage.DeleteTarget{{Location: objectURL}}); deleteErr != nil {
				result.Status = "error"
				result.Error = mapStorageDeleteError(deleteErr).Error()
			}
		}
		results = append(results, result)
	}
	return results
}

// DeleteProjectData performs the project cleanup sequence: catalog objects
// are removed first, then matching bucket scopes are listed and deleted in
// repository order. A scope count includes only successful deletions.
func (s *Service) DeleteProjectData(ctx context.Context, organization, project string) (ProjectCleanupResult, error) {
	result := ProjectCleanupResult{Organization: strings.TrimSpace(organization), ProjectID: strings.TrimSpace(project)}
	if s.cleanup.Objects == nil || s.cleanup.Scopes == nil {
		return result, &Error{Kind: ErrorUnsupported, Message: "project cleanup dependencies are not configured"}
	}
	deletedObjects, err := s.cleanup.Objects.DeleteBulkByScope(ctx, result.Organization, result.ProjectID)
	if err != nil {
		return result, err
	}
	result.DeletedObjects = deletedObjects
	scopes, err := s.cleanup.Scopes.ListBucketScopes(ctx)
	if err != nil {
		return result, err
	}
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Organization) != result.Organization || strings.TrimSpace(scope.ProjectID) != result.ProjectID {
			continue
		}
		credentialID := strings.TrimSpace(scope.CredentialID)
		if credentialID == "" {
			credentialID = strings.TrimSpace(scope.Bucket)
		}
		if credentialID == "" {
			continue
		}
		if err := s.cleanup.Scopes.DeleteBucketScope(ctx, result.Organization, result.ProjectID, credentialID, scope.PathPrefix); err != nil {
			return result, err
		}
		result.DeletedBucketScopes++
	}
	return result, nil
}

type deleteCandidate struct {
	provider string
	bucket   string
	key      string
}

func parseDeleteURL(ctx context.Context, service *Service, raw string) (deleteCandidate, string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return deleteCandidate{}, "", fmt.Errorf("parse access url %q: %w", raw, err)
	}
	provider := address.ProviderFromScheme(parsed.Scheme)
	if provider == "" {
		return deleteCandidate{}, "invalid", nil
	}
	bucket := strings.TrimSpace(parsed.Host)
	key := strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if bucket == "" || key == "" {
		return deleteCandidate{}, "invalid", nil
	}
	credential, err := service.credentialForBucket(ctx, bucket)
	if err != nil {
		return deleteCandidate{}, "", fmt.Errorf("lookup credential for bucket %s: %w", bucket, err)
	}
	if provider == address.S3Provider {
		provider = address.NormalizeProvider(credential.Provider, provider)
	}
	return deleteCandidate{provider: provider, bucket: bucket, key: key}, "valid", nil
}

func targetAllowed(candidate deleteCandidate, target scopeTarget) bool {
	if address.NormalizeProvider(candidate.provider, address.S3Provider) != address.S3Provider || !strings.EqualFold(candidate.bucket, target.Bucket) {
		return false
	}
	key := strings.Trim(strings.TrimSpace(candidate.key), "/")
	prefix := strings.Trim(strings.TrimSpace(target.Prefix), "/")
	if prefix == "" {
		return key != ""
	}
	return key == prefix || strings.HasPrefix(key, prefix+"/")
}

func uniqueURLs(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func mapStorageDeleteError(err error) error {
	var operation *storage.OperationError
	if errors.As(err, &operation) && operation.Cause != nil {
		return operation.Cause
	}
	return err
}

// DeleteObjectStorage deliberately retains the safety contract of the old
// facade. Physical replica deletion is not activated by maintenance moves.
func (s *Service) DeleteObjectStorage(context.Context, *objects.Record) error {
	return faults.ErrConflict
}

func (s *Service) DeleteObjectsStorage(context.Context, []objects.Record) error {
	return faults.ErrConflict
}

type requestCacheKey struct{}

type requestCache struct {
	mu            sync.Mutex
	credentials   map[string]credentialEntry
	visibleLoaded bool
	visibleValue  map[string]buckets.VisibleBucket
	visibleErr    error
	probes        map[string]ProbeResult
}

type credentialEntry struct {
	credential *buckets.Credential
	err        error
}

func withRequestCache(ctx context.Context) context.Context {
	if cacheFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, requestCacheKey{}, &requestCache{credentials: make(map[string]credentialEntry), probes: make(map[string]ProbeResult)})
}

func cacheFromContext(ctx context.Context) *requestCache {
	cache, _ := ctx.Value(requestCacheKey{}).(*requestCache)
	return cache
}

func cacheCredential(ctx context.Context, bucket string, credential *buckets.Credential, err error) {
	if cache := cacheFromContext(ctx); cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		var copyCredential *buckets.Credential
		if credential != nil {
			copy := *credential
			copyCredential = &copy
		}
		cache.credentials[strings.ToLower(strings.TrimSpace(bucket))] = credentialEntry{credential: copyCredential, err: err}
	}
}

func (cache *requestCache) credential(bucket string) (*buckets.Credential, error, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.credentials[strings.ToLower(strings.TrimSpace(bucket))]
	if !ok {
		return nil, nil, false
	}
	if entry.credential == nil {
		return nil, entry.err, true
	}
	copy := *entry.credential
	return &copy, entry.err, true
}

func cacheVisible(ctx context.Context, visible map[string]buckets.VisibleBucket, err error) {
	if cache := cacheFromContext(ctx); cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		cache.visibleLoaded = true
		cache.visibleValue = cloneVisible(visible)
		cache.visibleErr = err
	}
}

func (cache *requestCache) visible() (map[string]buckets.VisibleBucket, error, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if !cache.visibleLoaded {
		return nil, nil, false
	}
	return cloneVisible(cache.visibleValue), cache.visibleErr, true
}

func cloneVisible(input map[string]buckets.VisibleBucket) map[string]buckets.VisibleBucket {
	if input == nil {
		return nil
	}
	output := make(map[string]buckets.VisibleBucket, len(input))
	for key, value := range input {
		value.Programs = append([]string(nil), value.Programs...)
		output[key] = value
	}
	return output
}

func (cache *requestCache) probe(key string) (ProbeResult, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	result, ok := cache.probes[key]
	result.ValidationMismatches = append([]string(nil), result.ValidationMismatches...)
	return result, ok
}

func (cache *requestCache) setProbe(key string, result ProbeResult) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	result.ValidationMismatches = append([]string(nil), result.ValidationMismatches...)
	cache.probes[key] = result
}
