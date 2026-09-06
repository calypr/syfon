package core

import (
	"context"

	objectdomain "github.com/calypr/syfon/internal/objects"
)

// ObjectManager retains this forwarding surface while callers migrate to the
// objects service. Object behavior is implemented only by objects.Service.
func (m *ObjectManager) GetObject(ctx context.Context, ident, requiredMethod string) (*objectdomain.Record, error) {
	return m.objectService.GetObject(ctx, ident, requiredMethod)
}

func (m *ObjectManager) GetCanonicalContent(ctx context.Context, ident, requiredMethod string) (*objectdomain.CanonicalContent, error) {
	return m.objectService.GetCanonicalContent(ctx, ident, requiredMethod)
}

func (m *ObjectManager) GetObjectsByChecksums(ctx context.Context, hashes []string, requiredMethod string) (map[string][]objectdomain.Record, error) {
	return m.objectService.GetObjectsByChecksums(ctx, hashes, requiredMethod)
}

func (m *ObjectManager) GetObjectsByChecksum(ctx context.Context, checksum, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.GetObjectsByChecksum(ctx, checksum, requiredMethod)
}

func (m *ObjectManager) GetBulkObjects(ctx context.Context, ids []string, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.GetBulkObjects(ctx, ids, requiredMethod)
}

func (m *ObjectManager) GetPreparedScopedObjects(ctx context.Context, ids []string, organization, project, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.GetPreparedScopedObjects(ctx, ids, organization, project, requiredMethod)
}

func (m *ObjectManager) PrepareScopedObjects(ctx context.Context, records []objectdomain.Record, organization, project, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.PrepareScopedObjects(ctx, records, organization, project, requiredMethod)
}

func (m *ObjectManager) ListPreparedObjectsPageByScope(ctx context.Context, organization, project, requiredMethod, startAfter string, limit, offset int) ([]objectdomain.Record, error) {
	return m.objectService.ListPreparedObjectsPageByScope(ctx, organization, project, requiredMethod, startAfter, limit, offset)
}

func (m *ObjectManager) ListObjectIDsPageByChecksum(ctx context.Context, checksum, checksumType, organization, project, requiredMethod, startAfter string, limit, offset int) ([]string, error) {
	return m.objectService.ListObjectIDsPageByChecksum(ctx, checksum, checksumType, organization, project, requiredMethod, startAfter, limit, offset)
}

func (m *ObjectManager) ListObjectIDsPageByScope(ctx context.Context, organization, project, requiredMethod, startAfter string, limit, offset int) ([]string, error) {
	return m.objectService.ListObjectIDsPageByScope(ctx, organization, project, requiredMethod, startAfter, limit, offset)
}

func (m *ObjectManager) ListObjectIDsPageByURL(ctx context.Context, objectURL, organization, project, requiredMethod, startAfter string, limit, offset int) ([]string, error) {
	return m.objectService.ListObjectIDsPageByURL(ctx, objectURL, organization, project, requiredMethod, startAfter, limit, offset)
}

func (m *ObjectManager) ListObjectIDsByScope(ctx context.Context, organization, project, requiredMethod string) ([]string, error) {
	return m.objectService.ListObjectIDsByScope(ctx, organization, project, requiredMethod)
}

func (m *ObjectManager) ListObjectsByScope(ctx context.Context, organization, project, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.ListObjectsByScope(ctx, organization, project, requiredMethod)
}

func (m *ObjectManager) ListPhysicalObjectsByScope(ctx context.Context, organization, project, requiredMethod string) ([]objectdomain.Record, error) {
	return m.objectService.ListPhysicalObjectsByScope(ctx, organization, project, requiredMethod)
}

func (m *ObjectManager) ListMissingScopedSHA256(ctx context.Context, organization, project string, checksums []string) ([]string, error) {
	return m.objectService.ListMissingScopedSHA256(ctx, organization, project, checksums)
}
