package core

import (
	"context"
	"time"

	objectdomain "github.com/calypr/syfon/internal/objects"
)

// The ObjectManager mutation methods are transitional facades. Their
// implementation lives in objects.Service so authorization and persistence
// sequencing have one owner during the caller migration.
func (m *ObjectManager) RegisterBulk(ctx context.Context, candidates []objectdomain.Candidate) (int, error) {
	now := time.Now().UTC()
	toRegister := make([]objectdomain.Record, 0, len(candidates))
	for _, candidate := range candidates {
		record, err := CandidateToRecord(candidate, now)
		if err != nil {
			return 0, err
		}
		toRegister = append(toRegister, record)
	}
	if err := m.objectService.RegisterObjects(ctx, toRegister); err != nil {
		return 0, err
	}
	return len(toRegister), nil
}

func (m *ObjectManager) DeleteBulkByScope(ctx context.Context, organization, project string) (int, error) {
	return m.objectService.DeleteBulkByScope(ctx, organization, project)
}

func (m *ObjectManager) DeleteObject(ctx context.Context, id string) error {
	return m.objectService.DeleteObject(ctx, id)
}

type DeleteOptions struct {
	DeleteStorageData bool
}

func (m *ObjectManager) DeleteObjectWithOptions(ctx context.Context, id string, opts DeleteOptions) error {
	return m.objectService.DeleteObjectWithOptions(ctx, id, objectdomain.DeleteOptions{DeleteStorageData: opts.DeleteStorageData})
}

func (m *ObjectManager) BulkDeleteObjects(ctx context.Context, ids []string) error {
	return m.objectService.BulkDeleteObjects(ctx, ids)
}

func (m *ObjectManager) BulkDeleteObjectsWithOptions(ctx context.Context, ids []string, opts DeleteOptions) error {
	return m.objectService.BulkDeleteObjectsWithOptions(ctx, ids, objectdomain.DeleteOptions{DeleteStorageData: opts.DeleteStorageData})
}

func (m *ObjectManager) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []objectdomain.AccessMethod) error {
	return m.objectService.UpdateObjectAccessMethods(ctx, objectID, accessMethods)
}

func (m *ObjectManager) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]objectdomain.AccessMethod) error {
	return m.objectService.BulkUpdateAccessMethods(ctx, updates)
}

func (m *ObjectManager) RemoveObjectControlledAccess(ctx context.Context, objectID, resource string) (*objectdomain.Record, error) {
	return m.objectService.RemoveObjectControlledAccess(ctx, objectID, resource)
}

func (m *ObjectManager) RegisterObjects(ctx context.Context, records []objectdomain.Record) error {
	return m.objectService.RegisterObjects(ctx, records)
}

func (m *ObjectManager) CollapseProjectChecksumDuplicates(ctx context.Context, organization, project string) (int, error) {
	return m.objectService.CollapseProjectChecksumDuplicates(ctx, organization, project)
}

func (m *ObjectManager) ReplaceObjects(ctx context.Context, records []objectdomain.Record) error {
	return m.objectService.ReplaceObjects(ctx, records)
}

func (m *ObjectManager) DeleteObjectsByChecksums(ctx context.Context, hashes []string) (int, error) {
	return m.objectService.DeleteObjectsByChecksums(ctx, hashes)
}

func (m *ObjectManager) CreateObjectAlias(ctx context.Context, aliasID, canonicalID string) error {
	return m.objectService.CreateObjectAlias(ctx, aliasID, canonicalID)
}

func (m *ObjectManager) RequireObjectResources(ctx context.Context, method string, resources []string) error {
	return m.objectService.RequireObjectResources(ctx, method, resources)
}
