package core

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

const maxDeniedAccessResources = 25

// RegisterBulk saves multiple internal objects as a single logical operation.
func (m *ObjectManager) RegisterBulk(ctx context.Context, candidates []drs.DrsObjectCandidate) (int, error) {
	now := time.Now().UTC()
	toRegister := make([]models.InternalObject, 0, len(candidates))
	for _, c := range candidates {
		obj, err := CandidateToInternalObject(c, now)
		if err != nil {
			return 0, err
		}
		toRegister = append(toRegister, obj)
	}

	if err := m.RegisterObjects(ctx, toRegister); err != nil {
		return 0, err
	}
	return len(toRegister), nil
}

// DeleteBulkByScope removes all objects matching an organization/project scope after verifying permissions.
func (m *ObjectManager) DeleteBulkByScope(ctx context.Context, organization, project string) (int, error) {
	if err := m.requireScopeMethod(ctx, organization, project, objectMethodDelete); err != nil {
		return 0, err
	}

	ids, err := m.db.ListObjectIDsByScope(ctx, organization, project)
	if err != nil {
		return 0, err
	}

	toDelete := m.filterDeletableObjectIDs(ctx, ids)

	if len(toDelete) == 0 {
		return 0, nil
	}

	if err := m.db.BulkDeleteObjects(ctx, toDelete); err != nil {
		return 0, err
	}
	return len(toDelete), nil
}

func (m *ObjectManager) DeleteObject(ctx context.Context, id string) error {
	obj, err := m.db.GetObject(ctx, id)
	if err != nil {
		return err
	}
	if err := m.requireObjectMethod(ctx, obj, objectMethodDelete); err != nil {
		return err
	}
	return m.db.DeleteObject(ctx, id)
}

func (m *ObjectManager) BulkDeleteObjects(ctx context.Context, ids []string) error {
	toDelete, err := m.deletableObjectIDs(ctx, ids)
	if err != nil {
		return err
	}
	if len(toDelete) == 0 {
		return nil
	}
	return m.db.BulkDeleteObjects(ctx, toDelete)
}

func (m *ObjectManager) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	obj, err := m.db.GetObject(ctx, objectID)
	if err != nil {
		return err
	}
	if err := m.requireObjectMethod(ctx, obj, objectMethodUpdate); err != nil {
		return err
	}
	return m.db.UpdateObjectAccessMethods(ctx, objectID, accessMethods)
}

func (m *ObjectManager) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	for objectID := range updates {
		obj, err := m.db.GetObject(ctx, objectID)
		if err != nil {
			return err
		}
		if err := m.requireObjectMethod(ctx, obj, objectMethodUpdate); err != nil {
			return err
		}
	}
	return m.db.BulkUpdateAccessMethods(ctx, updates)
}

func (m *ObjectManager) RegisterObjects(ctx context.Context, objs []models.InternalObject) error {
	if err := m.bulkObjectMethodError(ctx, objs, objectMethodCreate); err != nil {
		return err
	}
	return m.db.RegisterObjects(ctx, objs)
}

func (m *ObjectManager) ReplaceObjects(ctx context.Context, objs []models.InternalObject) error {
	for i := range objs {
		existing, err := m.db.GetObject(ctx, objs[i].Id)
		if err != nil {
			return err
		}
		if err := m.requireObjectMethod(ctx, existing, objectMethodUpdate); err != nil {
			return err
		}
		if err := m.requireObjectMethod(ctx, &objs[i], objectMethodUpdate); err != nil {
			return err
		}
	}
	return m.db.RegisterObjects(ctx, objs)
}

func (m *ObjectManager) DeleteObjectsByChecksums(ctx context.Context, hashes []string) (int, error) {
	objectsByChecksum, err := m.db.GetObjectsByChecksums(ctx, hashes)
	if err != nil {
		return 0, err
	}
	seen := make(map[string]struct{})
	toDelete := make([]string, 0)
	for _, hash := range hashes {
		for _, obj := range objectsByChecksum[hash] {
			if _, ok := seen[obj.Id]; ok {
				continue
			}
			if !m.hasObjectMethod(ctx, &obj, objectMethodDelete) {
				continue
			}
			seen[obj.Id] = struct{}{}
			toDelete = append(toDelete, obj.Id)
		}
	}
	if len(toDelete) == 0 {
		return 0, nil
	}
	if err := m.db.BulkDeleteObjects(ctx, toDelete); err != nil {
		return 0, err
	}
	return len(toDelete), nil
}

func (m *ObjectManager) CreateObjectAlias(ctx context.Context, aliasID, canonicalID string) error {
	obj, err := m.db.GetObject(ctx, canonicalID)
	if err != nil {
		return err
	}
	if err := m.requireObjectMethod(ctx, obj, objectMethodUpdate); err != nil {
		return err
	}
	return m.db.CreateObjectAlias(ctx, aliasID, canonicalID)
}

func (m *ObjectManager) filterDeletableObjectIDs(ctx context.Context, ids []string) []string {
	toDelete, _ := m.deletableObjectIDs(ctx, ids)
	return toDelete
}

func (m *ObjectManager) deletableObjectIDs(ctx context.Context, ids []string) ([]string, error) {
	objects, err := m.db.GetBulkObjects(ctx, ids)
	if err != nil {
		return nil, err
	}
	filtered := m.filterObjectsByMethod(ctx, objects, objectMethodDelete)
	toDelete := make([]string, 0, len(filtered))
	for _, obj := range filtered {
		toDelete = append(toDelete, obj.Id)
	}
	return toDelete, nil
}

func (m *ObjectManager) RequireObjectResources(ctx context.Context, method string, resources []string) error {
	if strings.TrimSpace(method) == "" {
		return nil
	}
	if authz.HasObjectMethodAccess(ctx, method, resources) {
		return nil
	}
	return common.ErrUnauthorized
}

func (m *ObjectManager) requireScopeMethod(ctx context.Context, organization, project, method string) error {
	resource, err := syfoncommon.ResourcePath(organization, project)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resource) == "" {
		return common.ErrUnauthorized
	}
	return m.RequireObjectResources(ctx, method, []string{resource})
}

func (m *ObjectManager) requireObjectMethod(ctx context.Context, obj *models.InternalObject, method string) error {
	if m.hasObjectMethod(ctx, obj, method) {
		return nil
	}
	return common.ErrUnauthorized
}

func (m *ObjectManager) hasObjectMethod(ctx context.Context, obj *models.InternalObject, method string) bool {
	method = strings.TrimSpace(method)
	if method == "" {
		return true
	}
	return authz.HasObjectMethodAccess(ctx, method, ObjectAccessResources(obj))
}

func (m *ObjectManager) bulkObjectMethodError(ctx context.Context, objs []models.InternalObject, method string) error {
	resources := make(map[string]struct{})
	var firstDeniedID string
	deniedRecords := 0
	for i := range objs {
		if m.hasObjectMethod(ctx, &objs[i], method) {
			continue
		}
		deniedRecords++
		if firstDeniedID == "" {
			firstDeniedID = objs[i].Id
		}
		for _, resource := range ObjectAccessResources(&objs[i]) {
			if strings.TrimSpace(resource) == "" {
				continue
			}
			resources[resource] = struct{}{}
		}
	}
	if deniedRecords == 0 {
		return nil
	}

	resourceList := make([]string, 0, len(resources))
	for resource := range resources {
		resourceList = append(resourceList, resource)
	}
	sort.Strings(resourceList)

	truncated := 0
	if len(resourceList) > maxDeniedAccessResources {
		truncated = len(resourceList) - maxDeniedAccessResources
		resourceList = resourceList[:maxDeniedAccessResources]
	}

	return &common.AuthorizationError{
		Method:             method,
		RecordID:           firstDeniedID,
		Resources:          resourceList,
		DeniedRecords:      deniedRecords,
		TotalRecords:       len(objs),
		TruncatedResources: truncated,
	}
}
