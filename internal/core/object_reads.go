package core

import (
	"context"
	"strings"

	"github.com/calypr/syfon/internal/common"
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
	byChecksum, err := m.db.GetObjectsByChecksum(ctx, ident)
	if err != nil || len(byChecksum) == 0 {
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

func (m *ObjectManager) ListObjectIDsByScope(ctx context.Context, organization, project string, requiredMethod string) ([]string, error) {
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
