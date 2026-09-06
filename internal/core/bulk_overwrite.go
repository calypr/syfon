package core

import (
	"context"

	objectdomain "github.com/calypr/syfon/internal/objects"
)

// BulkOverwriteResult is kept as an alias for callers that still use the
// ObjectManager facade.
type BulkOverwriteResult = objectdomain.BulkOverwriteResult

var ErrBulkOverwriteConflict = objectdomain.ErrBulkOverwriteConflict

func (m *ObjectManager) BulkOverwriteObjects(ctx context.Context, organization, project string, candidates []objectdomain.Record) (BulkOverwriteResult, error) {
	return m.objectService.BulkOverwriteObjects(ctx, organization, project, candidates)
}
