package transfers

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/usage"
)

// AccessPort signs one storage access request. The storage package owns the
// request and result values; transfers only needs this one operation.
type AccessPort interface {
	Access(context.Context, storage.AccessRequest) (storage.Access, error)
}

// MultipartPort owns provider-specific multipart operations. Upload IDs are
// intentionally opaque and are never parsed or normalized by transfers.
type MultipartPort interface {
	BeginMultipart(context.Context, storage.ObjectTarget) (storage.UploadID, error)
	AccessMultipartPart(context.Context, storage.MultipartPartRequest) (storage.Access, error)
	CompleteMultipart(context.Context, storage.CompleteMultipartRequest) error
}

// ScopeReader resolves the bucket scope attached to an authorized object or
// an upload resource. It is deliberately narrower than buckets.Service.
type ScopeReader interface {
	LookupBucketScope(context.Context, string, string) (buckets.Scope, bool, error)
}

// CredentialReader lists configured credentials for legacy S3 URL mapping.
// Transfers does not need credential mutation or individual lookup behavior.
type CredentialReader interface {
	ListS3Credentials(context.Context) ([]buckets.Credential, error)
}

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

// Dependencies are the consumer-owned ports used by Service. Each field is
// optional until the corresponding workflow is invoked, which lets callers
// compose only the capabilities they expose.
type Dependencies struct {
	Access      AccessPort
	Multipart   MultipartPort
	Scopes      ScopeReader
	Credentials CredentialReader
	Pending     PendingStore
	Events      EventRecorder
}
