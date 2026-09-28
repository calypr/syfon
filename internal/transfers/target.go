package transfers

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
)

// CanonicalStorageTargetRequest describes the object-backed target selection
// used by upload signing and by repairable logical download URLs.
type CanonicalStorageTargetRequest struct {
	Object         *drs.DrsObject
	AccessURL      string
	Bucket         string
	Key            string
	PreferChecksum bool
}

// CanonicalStorageTarget is a provider-neutral target. URL is the canonical
// s3:// representation for bucket/key targets; non-S3 locations are returned
// with only URL populated by ResolveCanonicalStorageTarget.
type CanonicalStorageTarget struct {
	Bucket string
	Key    string
	URL    string
}

func (s *Service) resolveDownloadTarget(ctx context.Context, obj *drs.DrsObject, sourceURL string) (storage.Target, error) {
	canonical, err := s.ResolveCanonicalStorageTarget(ctx, CanonicalStorageTargetRequest{Object: obj, AccessURL: sourceURL})
	if err != nil {
		return storage.Target{}, err
	}
	return storageTargetFromCanonical(sourceURL, canonical), nil
}

func storageTargetFromCanonical(original string, canonical CanonicalStorageTarget) storage.Target {
	target := storage.Target{OriginalURL: strings.TrimSpace(original), CanonicalURL: strings.TrimSpace(canonical.URL), PhysicalBucket: strings.TrimSpace(canonical.Bucket), Key: strings.Trim(strings.TrimSpace(canonical.Key), "/")}
	if target.OriginalURL == "" {
		target.OriginalURL = target.CanonicalURL
	}
	parsed, err := address.ParseLocation(target.CanonicalURL)
	if err == nil {
		target.Provider = parsed.Provider
		if target.PhysicalBucket == "" {
			target.PhysicalBucket = parsed.Bucket
		}
		if target.Key == "" {
			target.Key = parsed.Key
		}
		target.Path = parsed.Path
	}
	if target.Provider == "" {
		parsed, _ = address.ParseLocation(target.OriginalURL)
		target.Provider = parsed.Provider
		target.Path = parsed.Path
		if target.PhysicalBucket == "" {
			target.PhysicalBucket = parsed.Bucket
		}
		if target.Key == "" {
			target.Key = parsed.Key
		}
	}
	if bucket := strings.TrimSpace(target.PhysicalBucket); bucket != "" {
		target.LookupCandidates = []string{bucket}
	}
	if target.LookupKey == "" && len(target.LookupCandidates) > 0 {
		target.LookupKey = target.LookupCandidates[0]
	}
	return target
}

func (s *Service) resolveScopedTarget(ctx context.Context, organization, project, key string) (storage.Target, error) {
	canonical, err := s.ResolveScopedUploadTarget(ctx, organization, project, key)
	if err != nil {
		return storage.Target{}, err
	}
	return storageTargetFromCanonical(canonical.URL, canonical), nil
}

// ResolveCanonicalStorageTarget selects the physical target for an object.
// Each controlled resource is resolved independently so sibling project
// prefixes are never combined. Unscoped non-S3 URLs pass through.
func (s *Service) ResolveCanonicalStorageTarget(ctx context.Context, req CanonicalStorageTargetRequest) (CanonicalStorageTarget, error) {
	obj := req.Object
	if obj == nil {
		return CanonicalStorageTarget{}, fmt.Errorf("object is required")
	}

	candidates, err := s.storageScopeCandidatesForObject(ctx, obj)
	if err != nil {
		return CanonicalStorageTarget{}, err
	}

	existingURL := strings.TrimSpace(req.AccessURL)
	if existingURL == "" {
		existingURL = firstSupportedAccessURL(obj)
	}
	existingBucket, existingKey, existingOK := parseS3Location(existingURL)

	if len(candidates) > 0 {
		candidate, err := selectObjectStorageScope(candidates, req.Bucket, existingBucket, req.Key, existingKey)
		if err != nil {
			return CanonicalStorageTarget{}, err
		}
		if candidate.bucket == "" {
			return CanonicalStorageTarget{}, fmt.Errorf("unable to resolve scoped storage bucket for object %s", obj.Id)
		}
		targetKey := canonicalObjectKey(obj, req.Key, existingKey, req.PreferChecksum)
		if existingOK && strings.EqualFold(strings.TrimSpace(existingBucket), candidate.bucket) && candidate.prefix == "" && strings.TrimSpace(existingKey) != "" {
			targetKey = existingKey
		}
		targetKey, err = normalizeScopedStorageKey(targetKey, candidate.scopes)
		if err != nil {
			return CanonicalStorageTarget{}, fmt.Errorf("%w: %v", errorapi.ErrInvalidInput, err)
		}
		if strings.TrimSpace(targetKey) == "" {
			return CanonicalStorageTarget{}, fmt.Errorf("unable to resolve scoped storage key for object %s", obj.Id)
		}
		return newCanonicalStorageTarget(candidate.bucket, targetKey), nil
	}

	if strings.TrimSpace(existingURL) == "" {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: object storage location is unavailable", errorapi.ErrInvalidInput)
	}
	if existingOK {
		if strings.TrimSpace(existingBucket) == "" || strings.TrimSpace(existingKey) == "" {
			return CanonicalStorageTarget{}, fmt.Errorf("%w: object storage location is invalid", errorapi.ErrInvalidInput)
		}
		return newCanonicalStorageTarget(existingBucket, existingKey), nil
	}
	return CanonicalStorageTarget{URL: existingURL}, nil
}

// ResolveScopedUploadTarget resolves an upload resource into its physical
// bucket/key. Organization and project scopes compose in lookup order.
func (s *Service) ResolveScopedUploadTarget(ctx context.Context, organization, project, key string) (CanonicalStorageTarget, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	key = strings.Trim(strings.TrimSpace(key), "/")
	if organization == "" {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: organization is required", errorapi.ErrInvalidInput)
	}
	if project != "" && organization == "" {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: organization required when project is specified", errorapi.ErrInvalidInput)
	}

	scopes := make([]buckets.Scope, 0, 2)
	if s.scopes != nil {
		if scope, found, err := s.scopes.LookupBucketScope(ctx, organization, ""); err != nil {
			return CanonicalStorageTarget{}, err
		} else if found {
			scopes = append(scopes, scope)
		}
		if project != "" {
			if scope, found, err := s.scopes.LookupBucketScope(ctx, organization, project); err != nil {
				return CanonicalStorageTarget{}, err
			} else if found {
				scopes = append(scopes, scope)
			}
		}
	}
	if len(scopes) == 0 {
		if project != "" {
			return CanonicalStorageTarget{}, fmt.Errorf("%w: no bucket scope configured for organization %q project %q", errorapi.ErrInvalidInput, organization, project)
		}
		return CanonicalStorageTarget{}, fmt.Errorf("%w: no bucket scope configured for organization %q", errorapi.ErrInvalidInput, organization)
	}

	bucket := ""
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Bucket) != "" {
			bucket = strings.TrimSpace(scope.Bucket)
		}
	}
	if bucket == "" {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: unable to resolve scoped storage bucket for organization %q project %q", errorapi.ErrInvalidInput, organization, project)
	}
	key, err := normalizeScopedStorageKey(key, scopes)
	if err != nil {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: %v", errorapi.ErrInvalidInput, err)
	}
	if key == "" {
		return CanonicalStorageTarget{}, fmt.Errorf("%w: unable to resolve scoped storage key for organization %q project %q", errorapi.ErrInvalidInput, organization, project)
	}
	return newCanonicalStorageTarget(bucket, key), nil
}

type objectStorageScopeCandidate struct {
	scopes []buckets.Scope
	bucket string
	prefix string
}

func (s *Service) storageScopeCandidatesForObject(ctx context.Context, obj *drs.DrsObject) ([]objectStorageScopeCandidate, error) {
	if obj == nil || s.scopes == nil {
		return nil, nil
	}
	resources := objects.AccessResources(obj)
	if len(resources) == 0 {
		return nil, nil
	}
	orgProjects := make(map[string]map[string]struct{})
	for _, resource := range resources {
		organization, project, ok := parseResourceScope(resource)
		if !ok {
			continue
		}
		if orgProjects[organization] == nil {
			orgProjects[organization] = make(map[string]struct{})
		}
		orgProjects[organization][project] = struct{}{}
	}
	organizations := make([]string, 0, len(orgProjects))
	for organization := range orgProjects {
		organizations = append(organizations, organization)
	}
	sort.Strings(organizations)
	candidates := make([]objectStorageScopeCandidate, 0, len(resources))
	for _, organization := range organizations {
		organizationScope, hasOrganizationScope, err := s.scopes.LookupBucketScope(ctx, organization, "")
		if err != nil {
			return nil, err
		}
		projects := make([]string, 0, len(orgProjects[organization]))
		for project := range orgProjects[organization] {
			projects = append(projects, project)
		}
		sort.Strings(projects)
		for _, project := range projects {
			scopes := make([]buckets.Scope, 0, 2)
			if hasOrganizationScope {
				scopes = append(scopes, organizationScope)
			}
			if project != "" {
				scope, found, err := s.scopes.LookupBucketScope(ctx, organization, project)
				if err != nil {
					return nil, err
				}
				if found {
					scopes = append(scopes, scope)
				}
			}
			if len(scopes) == 0 {
				continue
			}
			bucket := ""
			for _, scope := range scopes {
				if candidate := strings.TrimSpace(scope.Bucket); candidate != "" {
					bucket = candidate
				}
			}
			prefix := strings.Join(buckets.NormalizedStoragePrefixes(scopes), "/")
			candidates = append(candidates, objectStorageScopeCandidate{
				scopes: scopes,
				bucket: bucket,
				prefix: prefix,
			})
		}
	}
	return candidates, nil
}

func selectObjectStorageScope(candidates []objectStorageScopeCandidate, requestedBucket, existingBucket string, requestedKey, existingKey string) (objectStorageScopeCandidate, error) {
	selected := make([]int, 0, len(candidates))
	for i := range candidates {
		selected = append(selected, i)
	}
	matched := false
	applyMatch := func(matches []int) error {
		if len(matches) == 0 {
			return nil
		}
		if !matched {
			selected = matches
			matched = true
			return nil
		}
		intersection := make([]int, 0, len(selected))
		for _, current := range selected {
			for _, match := range matches {
				if current == match {
					intersection = append(intersection, current)
					break
				}
			}
		}
		if len(intersection) == 0 {
			return fmt.Errorf("%w: storage bucket and key identify different object scopes", errorapi.ErrInvalidInput)
		}
		selected = intersection
		return nil
	}

	requestedBucket = strings.TrimSpace(requestedBucket)
	if requestedBucket != "" {
		matches := matchingStorageBuckets(candidates, requestedBucket)
		if len(matches) == 0 {
			return objectStorageScopeCandidate{}, fmt.Errorf("%w: requested bucket %q is outside the object's storage scopes", errorapi.ErrInvalidInput, requestedBucket)
		}
		if err := applyMatch(matches); err != nil {
			return objectStorageScopeCandidate{}, err
		}
	}
	if existingBucket = strings.TrimSpace(existingBucket); existingBucket != "" {
		if err := applyMatch(matchingStorageBuckets(candidates, existingBucket)); err != nil {
			return objectStorageScopeCandidate{}, err
		}
	}
	for _, key := range uniqueNonEmptyStrings(requestedKey, existingKey) {
		if err := applyMatch(matchingStoragePrefixes(candidates, key)); err != nil {
			return objectStorageScopeCandidate{}, err
		}
	}

	unique := make([]int, 0, len(selected))
	type targetIdentity struct {
		bucket string
		prefix string
	}
	seen := make(map[targetIdentity]struct{}, len(selected))
	for _, index := range selected {
		candidate := candidates[index]
		identity := targetIdentity{bucket: candidate.bucket, prefix: candidate.prefix}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		unique = append(unique, index)
	}
	if len(unique) != 1 {
		return objectStorageScopeCandidate{}, fmt.Errorf("%w: ambiguous storage scope for object", errorapi.ErrInvalidInput)
	}
	return candidates[unique[0]], nil
}

func matchingStorageBuckets(candidates []objectStorageScopeCandidate, bucket string) []int {
	matches := make([]int, 0, len(candidates))
	for i, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.bucket), strings.TrimSpace(bucket)) {
			matches = append(matches, i)
		}
	}
	return matches
}

func matchingStoragePrefixes(candidates []objectStorageScopeCandidate, key string) []int {
	key = strings.Trim(strings.TrimSpace(key), "/")
	bestLength := 0
	matches := make([]int, 0)
	for i, candidate := range candidates {
		bestCandidateLength := 0
		prefixes := make([]string, 0, len(candidate.scopes)+1)
		prefixes = append(prefixes, candidate.prefix)
		for _, scope := range candidate.scopes {
			prefixes = append(prefixes, scope.PathPrefix)
		}
		for _, rawPrefix := range prefixes {
			prefix := strings.Trim(strings.TrimSpace(rawPrefix), "/")
			if prefix != "" && (key == prefix || strings.HasPrefix(key, prefix+"/")) && len(prefix) > bestCandidateLength {
				bestCandidateLength = len(prefix)
			}
		}
		if bestCandidateLength > bestLength {
			bestLength = bestCandidateLength
			matches = matches[:0]
		}
		if bestCandidateLength > 0 && bestCandidateLength == bestLength {
			matches = append(matches, i)
		}
	}
	if bestLength == 0 {
		// A bucket-wide scope remains a valid fallback when the existing object
		// key does not match any more specific project prefix.
		for i, candidate := range candidates {
			if strings.Trim(strings.TrimSpace(candidate.prefix), "/") == "" {
				matches = append(matches, i)
			}
		}
	}
	return matches
}

func uniqueNonEmptyStrings(values ...string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.Trim(strings.TrimSpace(value), "/")
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func canonicalObjectKey(obj *drs.DrsObject, explicitKey, existingKey string, preferChecksum bool) string {
	explicitKey = strings.Trim(strings.TrimSpace(explicitKey), "/")
	if explicitKey != "" {
		return explicitKey
	}
	checksum := ""
	if sha, ok := objects.CanonicalSHA256(obj.Checksums); ok {
		checksum = strings.Trim(strings.TrimSpace(sha), "/")
	}
	existingKey = strings.Trim(strings.TrimSpace(existingKey), "/")
	if preferChecksum {
		if checksum != "" {
			return checksum
		}
		if existingKey != "" {
			return existingKey
		}
	} else {
		if existingKey != "" {
			return existingKey
		}
		if checksum != "" {
			return checksum
		}
	}
	return strings.Trim(strings.TrimSpace(obj.Id), "/")
}

func newCanonicalStorageTarget(bucket, key string) CanonicalStorageTarget {
	bucket = strings.TrimSpace(bucket)
	key = strings.Trim(strings.TrimSpace(key), "/")
	return CanonicalStorageTarget{Bucket: bucket, Key: key, URL: address.BucketToURL(bucket, key)}
}

func firstSupportedAccessURL(obj *drs.DrsObject) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	for _, method := range *obj.AccessMethods {
		if method.AccessUrl == nil || strings.TrimSpace(method.AccessUrl.Url) == "" {
			continue
		}
		scheme := address.SchemeFromURL(method.AccessUrl.Url)
		if scheme != "" && address.ProviderFromScheme(scheme) == "" {
			continue
		}
		return method.AccessUrl.Url
	}
	return ""
}

func parseS3Location(accessURL string) (bucket, key string, ok bool) {
	if bucket, key, ok := address.ParseS3URL(accessURL); ok {
		return bucket, key, true
	}
	parsed, err := url.Parse(strings.TrimSpace(accessURL))
	if err != nil || !strings.EqualFold(strings.TrimSpace(parsed.Scheme), "s3") {
		return "", "", false
	}
	return strings.TrimSpace(parsed.Host), strings.Trim(strings.TrimSpace(parsed.Path), "/"), true
}

func normalizeScopedStorageKey(key string, scopes []buckets.Scope) (string, error) {
	key = strings.Trim(strings.TrimSpace(key), "/")
	if err := address.ValidateScopedKey(key); err != nil {
		return "", err
	}
	prefixes := buckets.NormalizedStoragePrefixes(scopes)
	remainder := key
	for _, prefix := range prefixes {
		remainder = address.TrimLeadingStoragePrefix(remainder, prefix)
	}
	composedPrefix := strings.Join(prefixes, "/")
	switch {
	case composedPrefix == "":
		return remainder, nil
	case remainder == "":
		return composedPrefix, nil
	default:
		return path.Join(composedPrefix, remainder), nil
	}
}

func parseResourceScope(resource string) (organization, project string, ok bool) {
	resource = strings.TrimSpace(resource)
	if parsed, err := url.Parse(resource); err == nil && parsed.Path != "" {
		resource = parsed.Path
	}
	parts := strings.Split(strings.Trim(resource, "/"), "/")
	if len(parts) < 2 {
		return "", "", false
	}
	if parts[0] != "organization" && parts[0] != "organizations" && parts[0] != "program" && parts[0] != "programs" {
		return "", "", false
	}
	organization = strings.TrimSpace(parts[1])
	if organization == "" {
		return "", "", false
	}
	if len(parts) >= 4 && (parts[2] == "project" || parts[2] == "projects") {
		project = strings.TrimSpace(parts[3])
	}
	return organization, project, true
}
