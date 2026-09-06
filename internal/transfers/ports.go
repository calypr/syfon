package transfers

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/usage"
)

// PendingMetadata is the plain value staged before an LFS object is verified.
// The candidate is already translated out of the generated HTTP contract.
type PendingMetadata struct {
	OID       string
	Candidate objects.Candidate
	CreatedAt time.Time
	ExpiresAt time.Time
}

// PendingStore persists staged LFS metadata and provides atomic consumption.
type PendingStore interface {
	SavePendingLFSMeta(ctx context.Context, entries []PendingMetadata) error
	GetPendingLFSMeta(ctx context.Context, oid string) (*PendingMetadata, error)
	PopPendingLFSMeta(ctx context.Context, oid string) (*PendingMetadata, error)
}

// EventRecorder is the narrow transfer-to-accounting boundary for access
// issuance events. Event and identity values remain owned by usage.
type EventRecorder interface {
	RecordTransferAttributionEvents(ctx context.Context, events []usage.Event) error
}
