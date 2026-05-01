package core

import (
	"context"
	"sort"
	"strings"

	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
)

// GetObject retrieves an internal object by ID, Alias, or Checksum and validates access.
func (m *ObjectManager) GetObject(ctx context.Context, ident string, requiredMethod string) (*models.InternalObject, error) {
	if strings.TrimSpace(ident) == "" {
		return nil, common.ErrNotFound
	}

	if obj, found, err := m.lookupObjectByID(ctx, ident); err != nil {
		return nil, err
	} else if found {
		return m.checkAccessAndReturn(obj, requiredMethod, ctx)
	}

	if obj, found, err := m.lookupObjectByAlias(ctx, ident); err != nil {
		return nil, err
	} else if found {
		return m.checkAccessAndReturn(obj, requiredMethod, ctx)
	}

	if obj, found, err := m.lookupObjectByChecksum(ctx, ident, requiredMethod); err != nil {
		return nil, err
	} else if found {
		return obj, nil
	}

	return nil, common.ErrNotFound
}

func (m *ObjectManager) lookupObjectByChecksum(ctx context.Context, ident string, requiredMethod string) (*models.InternalObject, bool, error) {
	if ids, optimized, err := m.authorizedChecksumIDs(ctx, ident, requiredMethod); err != nil {
		return nil, false, err
	} else if optimized {
		if len(ids) == 0 {
			if strings.TrimSpace(requiredMethod) != "" {
				if allMatches, ok, err := m.authorizedChecksumIDs(ctx, ident, ""); err != nil {
					return nil, false, err
				} else if ok && len(allMatches) > 0 {
					return nil, true, common.ErrUnauthorized
				}
			}
			return nil, false, nil
		}
		objects, err := m.db.GetBulkObjects(ctx, ids[:1])
		if err != nil {
			return nil, false, err
		}
		if len(objects) > 0 {
			return &objects[0], true, nil
		}
		return nil, false, nil
	}

	byChecksum, err := m.db.GetObjectsByChecksum(ctx, ident)
	if err != nil {
		return nil, false, err
	}
	if len(byChecksum) == 0 {
		return nil, false, nil
	}
	for i := range byChecksum {
		if m.hasObjectMethod(ctx, &byChecksum[i], requiredMethod) {
			return &byChecksum[i], true, nil
		}
	}
	if requiredMethod != "" {
		return nil, true, common.ErrUnauthorized
	}
	return nil, false, nil
}

func (m *ObjectManager) lookupObjectByID(ctx context.Context, ident string) (*models.InternalObject, bool, error) {
	obj, err := m.db.GetObject(ctx, ident)
	if err == nil {
		return obj, true, nil
	}
	if common.IsNotFoundError(err) {
		return nil, false, nil
	}
	return nil, false, err
}

func (m *ObjectManager) lookupObjectByAlias(ctx context.Context, ident string) (*models.InternalObject, bool, error) {
	canonicalID, aliasErr := m.db.ResolveObjectAlias(ctx, ident)
	if aliasErr != nil {
		if common.IsNotFoundError(aliasErr) {
			return nil, false, nil
		}
		return nil, false, aliasErr
	}
	if strings.TrimSpace(canonicalID) == "" {
		return nil, false, nil
	}

	obj, err := m.db.GetObject(ctx, canonicalID)
	if err != nil {
		if common.IsNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	objCopy := *obj
	objCopy.DrsObject.Id = ident
	objCopy.DrsObject.SelfUri = "drs://" + ident
	return &objCopy, true, nil
}

func (m *ObjectManager) checkAccessAndReturn(obj *models.InternalObject, method string, ctx context.Context) (*models.InternalObject, error) {
	if err := m.requireObjectMethod(ctx, obj, method); err != nil {
		return nil, err
	}
	return obj, nil
}

func (m *ObjectManager) GetObjectsByChecksums(ctx context.Context, hashes []string, requiredMethod string) (map[string][]models.InternalObject, error) {
	objectsByChecksum, err := m.db.GetObjectsByChecksums(ctx, hashes)
	if err != nil {
		return nil, err
	}
	filtered := make(map[string][]models.InternalObject, len(objectsByChecksum))
	for checksum, objects := range objectsByChecksum {
		filtered[checksum] = m.filterObjectsByMethod(ctx, objects, requiredMethod)
	}
	return filtered, nil
}

func (m *ObjectManager) GetObjectsByChecksum(ctx context.Context, checksum string, requiredMethod string) ([]models.InternalObject, error) {
	if ids, optimized, err := m.authorizedChecksumIDs(ctx, checksum, requiredMethod); err != nil {
		return nil, err
	} else if optimized {
		if len(ids) == 0 {
			return []models.InternalObject{}, nil
		}
		return m.db.GetBulkObjects(ctx, ids)
	}

	objects, err := m.db.GetObjectsByChecksum(ctx, checksum)
	if err != nil {
		return nil, err
	}
	return m.filterObjectsByMethod(ctx, objects, requiredMethod), nil
}

func (m *ObjectManager) GetBulkObjects(ctx context.Context, ids []string, requiredMethod string) ([]models.InternalObject, error) {
	objects, err := m.db.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}
	return m.filterObjectsByMethod(ctx, objects, requiredMethod), nil
}

func (m *ObjectManager) ListObjectIDsPageByChecksum(ctx context.Context, checksum, checksumType, organization, project, requiredMethod, startAfter string, limit, offset int) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	if pager, ok := m.db.(db.ObjectChecksumPageLister); ok && strings.EqualFold(strings.TrimSpace(requiredMethod), objectMethodRead) {
		resources, includeUnscoped, restrictToResources, ok := m.readableChecksumFilter(ctx, organization, project)
		if ok {
			return pager.ListObjectIDsPageByChecksum(ctx, checksum, checksumType, organization, project, startAfter, limit, offset, resources, includeUnscoped, restrictToResources)
		}
	}

	objects, err := m.GetObjectsByChecksum(ctx, checksum, requiredMethod)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(objects))
	for _, obj := range objects {
		if checksumType != "" && !common.ObjectHasChecksumTypeAndValue(obj, checksumType, checksum) {
			continue
		}
		if strings.TrimSpace(organization) != "" && !objectMatchesScope(&obj, organization, project) {
			continue
		}
		ids = append(ids, obj.Id)
	}
	sort.Strings(ids)
	if startAfter != "" {
		offset = searchAfterID(ids, startAfter)
	}
	if offset >= len(ids) {
		return []string{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

func (m *ObjectManager) ListObjectIDsPageByScope(ctx context.Context, organization, project, requiredMethod, startAfter string, limit, offset int) ([]string, error) {
	if limit <= 0 {
		return []string{}, nil
	}
	if pager, ok := m.db.(db.ObjectIDPageLister); ok {
		if strings.TrimSpace(organization) == "" && strings.EqualFold(strings.TrimSpace(requiredMethod), objectMethodRead) {
			if ids, ok, err := m.listReadableObjectIDsPage(ctx, startAfter, limit, offset); ok || err != nil {
				return ids, err
			}
			return pager.ListObjectIDsPageByScope(ctx, organization, project, startAfter, limit, offset)
		}
		if strings.EqualFold(strings.TrimSpace(requiredMethod), objectMethodRead) && m.canPageScopeRead(ctx, organization, project) {
			return pager.ListObjectIDsPageByScope(ctx, organization, project, startAfter, limit, offset)
		}
	}

	ids, err := m.ListObjectIDsByScope(ctx, organization, project, requiredMethod)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return []string{}, nil
	}
	sort.Strings(ids)
	if startAfter != "" {
		offset = searchAfterID(ids, startAfter)
	}
	if offset >= len(ids) {
		return []string{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

func (m *ObjectManager) ListObjectIDsByScope(ctx context.Context, organization, project string, requiredMethod string) ([]string, error) {
	if strings.TrimSpace(organization) == "" && strings.EqualFold(strings.TrimSpace(requiredMethod), objectMethodRead) {
		if ids, ok, err := m.listReadableObjectIDs(ctx); ok || err != nil {
			return ids, err
		}
	}
	ids, err := m.db.ListObjectIDsByScope(ctx, organization, project)
	if err != nil {
		return nil, err
	}
	objects, err := m.db.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}
	filtered := m.filterObjectsByMethod(ctx, objects, requiredMethod)
	out := make([]string, 0, len(filtered))
	for _, obj := range filtered {
		out = append(out, obj.Id)
	}
	return out, nil
}

func (m *ObjectManager) listReadableObjectIDs(ctx context.Context) ([]string, bool, error) {
	lister, ok := m.db.(db.ObjectIDResourceLister)
	if !ok || !authz.IsAuthzEnforced(ctx) {
		return nil, false, nil
	}
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return []string{}, true, nil
	}

	resources := readableResources(ctx)
	ids, err := lister.ListObjectIDsByResources(ctx, resources, true)
	return ids, true, err
}

func (m *ObjectManager) listReadableObjectIDsPage(ctx context.Context, startAfter string, limit, offset int) ([]string, bool, error) {
	pager, ok := m.db.(db.ObjectIDPageLister)
	if !ok || !authz.IsAuthzEnforced(ctx) {
		return nil, false, nil
	}
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return []string{}, true, nil
	}

	resources := readableResources(ctx)
	ids, err := pager.ListObjectIDsPageByResources(ctx, resources, true, startAfter, limit, offset)
	return ids, true, err
}

func (m *ObjectManager) canPageScopeRead(ctx context.Context, organization, project string) bool {
	if !authz.IsAuthzEnforced(ctx) {
		return true
	}
	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return false
	}
	return authz.HasMethodAccess(ctx, objectMethodRead, []string{resource})
}

func readableResources(ctx context.Context) []string {
	return authorizedResources(ctx, objectMethodRead)
}

func (m *ObjectManager) readableChecksumFilter(ctx context.Context, organization, project string) ([]string, bool, bool, bool) {
	if !authz.IsAuthzEnforced(ctx) {
		return nil, false, false, true
	}
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return nil, false, true, true
	}
	if authz.HasMethodAccess(ctx, objectMethodRead, []string{"/programs"}) || authz.HasMethodAccess(ctx, objectMethodRead, []string{"/data_file"}) {
		return nil, false, false, true
	}
	if strings.TrimSpace(organization) != "" && m.canPageScopeRead(ctx, organization, project) {
		return nil, false, false, true
	}
	return readableResources(ctx), true, true, true
}

func objectMethodResourceFilter(ctx context.Context, method string) ([]string, bool, bool) {
	method = strings.TrimSpace(method)
	if method == "" || !authz.IsAuthzEnforced(ctx) {
		return nil, true, false
	}
	if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
		return nil, false, true
	}
	if authz.HasMethodAccess(ctx, method, []string{"/programs"}) || authz.HasMethodAccess(ctx, method, []string{"/data_file"}) {
		return nil, strings.EqualFold(method, objectMethodRead), false
	}
	return authorizedResources(ctx, method), strings.EqualFold(method, objectMethodRead), true
}

func authorizedResources(ctx context.Context, method string) []string {
	privileges := authz.GetUserPrivileges(ctx)
	if len(privileges) == 0 {
		return syfoncommon.NormalizeAccessResources(authz.GetUserAuthz(ctx))
	}
	resources := make([]string, 0, len(privileges))
	for resource, methods := range privileges {
		if methods[method] || methods["*"] {
			resources = append(resources, resource)
		}
	}
	return syfoncommon.NormalizeAccessResources(resources)
}

func (m *ObjectManager) authorizedChecksumIDs(ctx context.Context, checksum, requiredMethod string) ([]string, bool, error) {
	lister, ok := m.db.(db.ObjectAuthorizedLister)
	if !ok {
		return nil, false, nil
	}
	resources, includeUnscoped, restrictToResources := objectMethodResourceFilter(ctx, requiredMethod)
	byChecksum, err := lister.ListObjectIDsByChecksumsAndResources(ctx, []string{checksum}, resources, includeUnscoped, restrictToResources)
	if err != nil {
		return nil, false, err
	}
	return byChecksum[checksum], true, nil
}

func searchAfterID(ids []string, startAfter string) int {
	idx := sort.SearchStrings(ids, startAfter)
	for idx < len(ids) && ids[idx] <= startAfter {
		idx++
	}
	return idx
}

func objectMatchesScope(obj *models.InternalObject, organization, project string) bool {
	if obj == nil || strings.TrimSpace(organization) == "" {
		return obj != nil
	}
	projects, ok := obj.Authorizations[organization]
	if !ok {
		return false
	}
	if strings.TrimSpace(project) == "" || len(projects) == 0 {
		return true
	}
	for _, p := range projects {
		if p == project {
			return true
		}
	}
	return false
}

func (m *ObjectManager) filterObjectsByMethod(ctx context.Context, objects []models.InternalObject, method string) []models.InternalObject {
	if strings.TrimSpace(method) == "" {
		return objects
	}
	filtered := make([]models.InternalObject, 0, len(objects))
	for _, obj := range objects {
		if m.hasObjectMethod(ctx, &obj, method) {
			filtered = append(filtered, obj)
		}
	}
	return filtered
}
