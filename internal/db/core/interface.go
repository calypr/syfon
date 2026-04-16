package core

import (
	"context"
	"time"

	"github.com/calypr/syfon/apigen/drs"
)

// ServiceInfoStore exposes service metadata reads.
type ServiceInfoStore interface {
	GetServiceInfo(ctx context.Context) (*drs.Service, error)
}

// ObjectStore groups the object lifecycle and lookup capabilities used by the API layers.
type ObjectStore interface {
	GetObject(ctx context.Context, id string) (*InternalObject, error)
	DeleteObject(ctx context.Context, id string) error
	CreateObject(ctx context.Context, obj *InternalObject) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]InternalObject, error)
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]InternalObject, error)
	ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error)
	CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error
	ResolveObjectAlias(ctx context.Context, aliasID string) (string, error)
	GetBulkObjects(ctx context.Context, ids []string) ([]InternalObject, error)
	BulkDeleteObjects(ctx context.Context, ids []string) error
	RegisterObjects(ctx context.Context, objects []InternalObject) error
	UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error
	BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error
}

// CredentialStore groups bucket credential and scope management.
type CredentialStore interface {
	GetS3Credential(ctx context.Context, bucket string) (*S3Credential, error)
	ListS3Credentials(ctx context.Context) ([]S3Credential, error)
	SaveS3Credential(ctx context.Context, cred *S3Credential) error
	DeleteS3Credential(ctx context.Context, bucket string) error
	CreateBucketScope(ctx context.Context, scope *BucketScope) error
	GetBucketScope(ctx context.Context, organization, projectID string) (*BucketScope, error)
	ListBucketScopes(ctx context.Context) ([]BucketScope, error)
}

// ObjectsAPIServiceDatabase is the storage surface used by the object service package.
type ObjectsAPIServiceDatabase interface {
	ServiceInfoStore
	ObjectStore
	CredentialStore
	UsageStore
}

// PendingLFSMetaStore manages pending LFS metadata.
type PendingLFSMetaStore interface {
	SavePendingLFSMeta(ctx context.Context, entries []PendingLFSMeta) error
	GetPendingLFSMeta(ctx context.Context, oid string) (*PendingLFSMeta, error)
	PopPendingLFSMeta(ctx context.Context, oid string) (*PendingLFSMeta, error)
}

// UsageStore manages file usage counters and summaries.
type UsageStore interface {
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
	GetFileUsage(ctx context.Context, objectID string) (*FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (FileUsageSummary, error)
}

// SHA256ValidityStore is the minimum storage surface needed by the SHA256 validity endpoint.
type SHA256ValidityStore interface {
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]InternalObject, error)
	ListS3Credentials(ctx context.Context) ([]S3Credential, error)
}

// MetricsStore is the minimum storage surface needed by the metrics API.
type MetricsStore interface {
	ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error)
	GetObject(ctx context.Context, id string) (*InternalObject, error)
	GetFileUsage(ctx context.Context, objectID string) (*FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (FileUsageSummary, error)
}

// LFSStore is the minimum storage surface needed by the LFS API.
type LFSStore interface {
	ObjectStore
	CredentialStore
	PendingLFSMetaStore
	UsageStore
}

// DatabaseInterface defines the full database backend contract.
type DatabaseInterface interface {
	ServiceInfoStore
	ObjectStore
	CredentialStore
	PendingLFSMetaStore
	UsageStore
}
