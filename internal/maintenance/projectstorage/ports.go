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

// ObjectScopeDeleter and ScopeCatalog are the two narrow capabilities needed
// by project cleanup. They deliberately avoid handing a database aggregate or
// a generated HTTP service to this maintenance package.
type ObjectScopeDeleter interface {
	DeleteBulkByScope(context.Context, string, string) (int, error)
}

type ScopeCatalog interface {
	ListBucketScopes(context.Context) ([]buckets.Scope, error)
	DeleteBucketScope(context.Context, string, string, string, string) error
}

type CleanupDependencies struct {
	Objects ObjectScopeDeleter
	Scopes  ScopeCatalog
}

// ServiceDependencies makes composition explicit while keeping each port
// consumer-owned. A single buckets.Service or storage.Manager may satisfy
// several ports through structural typing at the composition boundary.
