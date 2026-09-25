package objects

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	clientaccess "github.com/calypr/syfon/client/access"
	"github.com/calypr/syfon/internal/access"
)

type RepairCandidateQuery struct {
	Scope      Scope
	Bucket     string
	Prefix     string
	StartAfter string
	Limit      int
}

type RepairCandidatePage struct {
	Objects        []drs.DrsObject
	Scanned        int
	NextStartAfter string
}

type repairCandidateObjectStore interface {
	ListRepairCandidateObjectIDs(context.Context, RepairCandidateQuery) ([]string, error)
}

// ObjectIDPageQuery selects one deterministic page of object IDs. An empty
// ObjectURL pages by scope; a non-empty URL also filters by the access method.
type ObjectIDPageQuery struct {
	Scope                      Scope
	ObjectURL                  string
	StartAfter                 string
	Limit                      int
	Offset                     int
	VisibleResources           []string
	IncludeUnscoped            bool
	RestrictToVisibleResources bool
}

// ListRepairCandidates returns checksum-identified records whose access rows
// are missing but whose stored S3 URL belongs to the requested project scope.
func (s *Service) ListRepairCandidates(ctx context.Context, query RepairCandidateQuery) (RepairCandidatePage, error) {
	scope, err := NewScope(query.Scope.Organization, query.Scope.Project)
	if err != nil {
		return RepairCandidatePage{}, err
	}
	if scope.Organization == "" || scope.Project == "" {
		return RepairCandidatePage{}, fmt.Errorf("%w: repair candidates require an organization and project", errorapi.ErrInvalidInput)
	}
	if query.Limit < 0 {
		return RepairCandidatePage{}, fmt.Errorf("%w: repair candidate limit must be >= 0", errorapi.ErrInvalidInput)
	}
	if query.Limit == 0 {
		return RepairCandidatePage{}, nil
	}
	query.Scope = scope
	query.Bucket = strings.TrimSpace(query.Bucket)
	query.Prefix = strings.Trim(strings.TrimSpace(query.Prefix), "/")
	query.StartAfter = strings.TrimSpace(query.StartAfter)
	if query.Bucket == "" {
		return RepairCandidatePage{}, fmt.Errorf("%w: repair candidate bucket is required", errorapi.ErrInvalidInput)
	}
	resource, err := clientaccess.ResourcePath(scope.Organization, scope.Project)
	if err != nil {
		return RepairCandidatePage{}, err
	}
	if access.IsAuthzEnforced(ctx) &&
		!access.HasObjectMethodAccess(ctx, objectMethodRead, []string{resource}) &&
		!access.HasMethodAccess(ctx, objectMethodRead, []string{"/programs"}) &&
		!access.HasMethodAccess(ctx, objectMethodRead, []string{"/data_file"}) {
		return RepairCandidatePage{}, errorapi.ErrAccessDenied
	}
	store, ok := s.store.(repairCandidateObjectStore)
	if !ok {
		return RepairCandidatePage{}, fmt.Errorf("repair candidate persistence is not configured")
	}
	ids, err := store.ListRepairCandidateObjectIDs(ctx, query)
	if err != nil {
		return RepairCandidatePage{}, err
	}
	page := RepairCandidatePage{Scanned: len(ids)}
	if len(ids) == 0 {
		return page, nil
	}
	page.NextStartAfter = ids[len(ids)-1]
	records, err := s.store.GetBulkObjects(ctx, ids)
	if err != nil {
		return RepairCandidatePage{}, err
	}
	for _, record := range records {
		hasResource := false
		for _, existing := range AccessResources(&record) {
			if existing == resource {
				hasResource = true
				break
			}
		}
		if hasResource {
			continue
		}
		sha, ok := CanonicalSHA256(record.Checksums)
		if !ok {
			continue
		}
		id, err := MintRecordIDFromChecksum(sha, []string{resource})
		if err == nil && id == record.Id {
			page.Objects = append(page.Objects, record)
		}
	}
	return page, nil
}

// ListObjects returns one authorized page after project checksum-family merging.
func (s *Service) ListObjects(ctx context.Context, query RecordListQuery) ([]drs.DrsObject, error) {
	scope, err := NewScope(query.Scope.Organization, query.Scope.Project)
	if err != nil {
		return nil, err
	}
	query.Scope = scope
	query.ObjectURL = strings.TrimSpace(query.ObjectURL)
	query.StartAfter = strings.TrimSpace(query.StartAfter)
	if query.Limit < 0 {
		return nil, fmt.Errorf("limit must be >= 0")
	}
	offset := 0
	if query.StartAfter == "" {
		if query.Page < 0 {
			return nil, fmt.Errorf("page must be >= 0")
		}
		if query.Limit > 0 && query.Page > int(^uint(0)>>1)/query.Limit {
			return nil, fmt.Errorf("page offset is too large")
		}
		offset = query.Page * query.Limit
	}
	if query.Limit > 0 && offset > int(^uint(0)>>1)-query.Limit {
		return nil, fmt.Errorf("page window is too large")
	}
	if query.Limit == 0 {
		return []drs.DrsObject{}, nil
	}

	if query.Checksum != nil {
		checksumType, checksum := ParseHashQuery(query.Checksum.Value, query.Checksum.Type)
		objectsByChecksum, err := s.store.GetObjectsByChecksums(ctx, []string{checksum})
		if err != nil {
			return nil, err
		}
		records := objectsByChecksum[checksum]
		if scope.Organization == "" {
			policy, err := s.publicReadPolicy(ctx, records)
			if err != nil {
				return nil, err
			}
			records = filterObjectsByMethod(ctx, canonicalizeContentObjects(objectsWithSHA256(records, checksum), policy), query.RequiredMethod, policy)
		} else {
			scoped := make([]drs.DrsObject, 0, len(records))
			for _, record := range records {
				if objectMatchesScope(&record, scope.Organization, scope.Project) {
					scoped = append(scoped, record)
				}
			}
			policy, err := s.publicReadPolicy(ctx, scoped)
			if err != nil {
				return nil, err
			}
			records = canonicalizeProjectScopedObjects(filterObjectsByMethod(ctx, scoped, query.RequiredMethod, policy), scope.Organization, scope.Project, policy)
		}
		ids := make([]string, 0, len(records))
		for _, record := range records {
			if checksumType != "" && !RecordHasChecksumTypeAndValue(record, checksumType, checksum) {
				continue
			}
			if scope.Organization != "" && !objectMatchesScope(&record, scope.Organization, scope.Project) {
				continue
			}
			ids = append(ids, record.Id)
		}
		return s.loadScopedRecords(ctx, pageIDs(ids, query.StartAfter, query.Limit, offset), query)
	}
	if query.ObjectURL != "" {
		resources, unscoped, restricted := objectMethodResourceFilter(ctx, query.RequiredMethod)
		if access.IsGen3Mode(ctx) && access.IsAuthzEnforced(ctx) && !access.HasAuthHeader(ctx) {
			return []drs.DrsObject{}, nil
		}
		ids, err := s.store.ListObjectIDsPage(ctx, ObjectIDPageQuery{
			Scope:                      scope,
			ObjectURL:                  query.ObjectURL,
			StartAfter:                 query.StartAfter,
			Limit:                      query.Limit,
			Offset:                     offset,
			VisibleResources:           resources,
			IncludeUnscoped:            unscoped,
			RestrictToVisibleResources: restricted,
		})
		if err != nil {
			return nil, err
		}
		return s.loadScopedRecords(ctx, ids, query)
	}
	return s.listScopeRecords(ctx, query, offset)
}

func (s *Service) loadScopedRecords(ctx context.Context, ids []string, query RecordListQuery) ([]drs.DrsObject, error) {
	records, err := s.store.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}
	return s.prepareScopedRecords(ctx, records, query.Scope, query.RequiredMethod)
}

func (s *Service) prepareScopedRecords(ctx context.Context, records []drs.DrsObject, scope Scope, method string) ([]drs.DrsObject, error) {
	expanded, err := s.expandProjectChecksumSiblingObjects(ctx, records, scope.Organization, scope.Project)
	if err != nil {
		return nil, err
	}
	policy, err := s.publicReadPolicy(ctx, expanded)
	if err != nil {
		return nil, err
	}
	return canonicalizeProjectScopedObjects(filterObjectsByMethod(ctx, expanded, method, policy), scope.Organization, scope.Project, policy), nil
}

func (s *Service) listScopeRecords(ctx context.Context, query RecordListQuery, offset int) ([]drs.DrsObject, error) {
	resources, unscoped, restricted := objectMethodResourceFilter(ctx, query.RequiredMethod)
	// Broad write access has no row-level visibility filter, so canonicalize all
	// matching checksum siblings before applying pagination.
	materializeCanonicalIDs := !restricted && !unscoped
	var canonicalIDs []string
	if materializeCanonicalIDs {
		var err error
		canonicalIDs, err = s.ListObjectIDsByScope(ctx, query.Scope.Organization, query.Scope.Project, query.RequiredMethod)
		if err != nil {
			return nil, err
		}
	}
	target := query.Limit + offset
	batchSize := query.Limit
	if batchSize < 100 {
		batchSize = 100
	}
	rawStart := query.StartAfter
	collected := make([]drs.DrsObject, 0)
	seen := make(map[string]struct{})
	for len(collected) < target {
		var ids []string
		if materializeCanonicalIDs {
			ids = pageIDs(canonicalIDs, rawStart, batchSize, 0)
		} else {
			var err error
			ids, err = s.store.ListObjectIDsPage(ctx, ObjectIDPageQuery{
				Scope:                      query.Scope,
				StartAfter:                 rawStart,
				Limit:                      batchSize,
				VisibleResources:           resources,
				IncludeUnscoped:            unscoped,
				RestrictToVisibleResources: restricted,
			})
			if err != nil {
				return nil, err
			}
		}
		if len(ids) == 0 {
			break
		}
		rawStart = ids[len(ids)-1]
		records, err := s.loadScopedRecords(ctx, ids, query)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if query.StartAfter != "" && record.Id <= query.StartAfter {
				continue
			}
			if _, ok := seen[record.Id]; ok {
				continue
			}
			seen[record.Id] = struct{}{}
			collected = append(collected, record)
			if len(collected) == target {
				break
			}
		}
		if len(ids) < batchSize {
			break
		}
	}
	if offset >= len(collected) {
		return []drs.DrsObject{}, nil
	}
	return collected[offset:], nil
}

func pageIDs(ids []string, start string, limit, offset int) []string {
	sort.Strings(ids)
	if start != "" {
		offset = searchAfterID(ids, start)
	}
	if offset >= len(ids) {
		return []string{}
	}
	return ids[offset : offset+min(limit, len(ids)-offset)]
}
