package projectstorage

import (
	"context"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
)

// ScopeReader is deliberately smaller than buckets.Service. It supplies the
// two scope lookups needed by project maintenance and leaves catalog policy in
// the buckets domain.
type ScopeReader interface {
	LookupBucketScope(context.Context, string, string) (buckets.Scope, bool, error)
}

type CredentialReader interface {
	GetS3Credential(context.Context, string) (*buckets.Credential, error)
	ListS3Credentials(context.Context) ([]buckets.Credential, error)
}

type VisibilityReader interface {
	ListVisibleBuckets(context.Context) (map[string]buckets.VisibleBucket, error)
}

type InventoryPort interface {
	Inventory(context.Context, storage.InventoryRequest) (storage.InventoryResult, error)
}

type ProbePort interface {
	Probe(context.Context, []storage.ProbeTarget) []storage.ProbeResult
}

type DeletePort interface {
	DeleteExact(context.Context, []storage.DeleteTarget) error
}

// ServiceDependencies makes composition explicit while keeping each port
// consumer-owned. A single buckets.Service or storage.Manager may satisfy
// several ports through structural typing at the composition boundary.
