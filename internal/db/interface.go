package db

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

// ObjectStore groups the object lifecycle and lookup capabilities used by the API layers.
type ObjectStore interface {
	GetObject(ctx context.Context, id string) (*objects.Record, error)
	DeleteObject(ctx context.Context, id string) error
	DeleteObjectAlias(ctx context.Context, aliasID string) error
	CreateObject(ctx context.Context, obj *objects.Record) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]objects.Record, error)
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]objects.Record, error)
	ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error)
	ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error)
	CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error
	ResolveObjectAlias(ctx context.Context, aliasID string) (string, error)
	GetBulkObjects(ctx context.Context, ids []string) ([]objects.Record, error)
	BulkDeleteObjects(ctx context.Context, ids []string) error
	RegisterObjects(ctx context.Context, objects []objects.Record) error
	ReplaceObjects(ctx context.Context, objects []objects.Record) error
	UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []objects.AccessMethod) error
	BulkUpdateAccessMethods(ctx context.Context, updates map[string][]objects.AccessMethod) error
	RemoveObjectControlledAccess(ctx context.Context, objectID, resource string) error
	RemoveObjectControlledAccessBulk(ctx context.Context, objectIDs []string, resource string) (int, error)
}

// CredentialStore groups bucket credential and scope management.
type CredentialStore interface {
	GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error)
	ListS3Credentials(ctx context.Context) ([]buckets.Credential, error)
	SaveS3Credential(ctx context.Context, cred *buckets.Credential) error
	DeleteS3Credential(ctx context.Context, bucket string) error
	CreateBucketScope(ctx context.Context, scope *buckets.Scope) error
	DeleteBucketScope(ctx context.Context, organization, projectID, credentialID, pathPrefix string) error
	GetBucketScope(ctx context.Context, organization, projectID string) (*buckets.Scope, error)
	ListBucketScopes(ctx context.Context) ([]buckets.Scope, error)
}

// ObjectsAPIServiceDatabase is the storage surface used by the object service package.
type ObjectsAPIServiceDatabase interface {
	ObjectStore
	CredentialStore
	UsageStore
}

// UsageStore manages file usage counters and summaries.
type UsageStore interface {
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
	RecordTransferAttributionEvents(ctx context.Context, events []usage.Event) error
	RecordProviderTransferEvents(ctx context.Context, events []usage.ProviderEvent) error
	GetTransferAttributionSummary(ctx context.Context, filter usage.Filter) (usage.Summary, error)
	GetTransferAttributionBreakdown(ctx context.Context, filter usage.Filter, groupBy string) ([]usage.Breakdown, error)
	GetFileUsage(ctx context.Context, objectID string) (*usage.FileUsage, error)
	ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]usage.FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]usage.FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (usage.FileUsageSummary, error)
}

// SHA256ValidityStore is the minimum storage surface needed by the SHA256 validity endpoint.
type SHA256ValidityStore interface {
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]objects.Record, error)
	ListS3Credentials(ctx context.Context) ([]buckets.Credential, error)
}

// MetricsStore is the minimum storage surface needed by the metrics API.
type MetricsStore interface {
	ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error)
	GetObject(ctx context.Context, id string) (*objects.Record, error)
	RecordTransferAttributionEvents(ctx context.Context, events []usage.Event) error
	RecordProviderTransferEvents(ctx context.Context, events []usage.ProviderEvent) error
	ListS3Credentials(ctx context.Context) ([]buckets.Credential, error)
	GetTransferAttributionSummary(ctx context.Context, filter usage.Filter) (usage.Summary, error)
	GetTransferAttributionBreakdown(ctx context.Context, filter usage.Filter, groupBy string) ([]usage.Breakdown, error)
	GetFileUsage(ctx context.Context, objectID string) (*usage.FileUsage, error)
	ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]usage.FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]usage.FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (usage.FileUsageSummary, error)
}

// LFSStore is the minimum storage surface needed by the LFS API.
type LFSStore interface {
	ObjectStore
	CredentialStore
	transfers.PendingStore
	UsageStore
}

// DatabaseInterface defines the full database backend contract.
type DatabaseInterface interface {
	ObjectStore
	CredentialStore
	transfers.PendingStore
	UsageStore
}
