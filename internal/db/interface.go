package db

import (
	"github.com/calypr/syfon/internal/models"
	"context"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

// ServiceInfoStore exposes service metadata reads.
type ServiceInfoStore interface {
	GetServiceInfo(ctx context.Context) (*drs.Service, error)
}

// ObjectStore groups the object lifecycle and lookup capabilities used by the API layers.
type ObjectStore interface {
	GetObject(ctx context.Context, id string) (*models.InternalObject, error)
	DeleteObject(ctx context.Context, id string) error
	CreateObject(ctx context.Context, obj *models.InternalObject) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]models.InternalObject, error)
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]models.InternalObject, error)
	ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error)
	CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error
	ResolveObjectAlias(ctx context.Context, aliasID string) (string, error)
	GetBulkObjects(ctx context.Context, ids []string) ([]models.InternalObject, error)
	BulkDeleteObjects(ctx context.Context, ids []string) error
	RegisterObjects(ctx context.Context, objects []models.InternalObject) error
	UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error
	BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error
}

// CredentialStore groups bucket credential and scope management.
type CredentialStore interface {
	GetS3Credential(ctx context.Context, bucket string) (*models.S3Credential, error)
	ListS3Credentials(ctx context.Context) ([]models.S3Credential, error)
	SaveS3Credential(ctx context.Context, cred *models.S3Credential) error
	DeleteS3Credential(ctx context.Context, bucket string) error
	CreateBucketScope(ctx context.Context, scope *models.BucketScope) error
	GetBucketScope(ctx context.Context, organization, projectID string) (*models.BucketScope, error)
	ListBucketScopes(ctx context.Context) ([]models.BucketScope, error)
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
	SavePendingLFSMeta(ctx context.Context, entries []models.PendingLFSMeta) error
	GetPendingLFSMeta(ctx context.Context, oid string) (*models.PendingLFSMeta, error)
	PopPendingLFSMeta(ctx context.Context, oid string) (*models.PendingLFSMeta, error)
}

// UsageStore manages file usage counters and summaries.
type UsageStore interface {
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
	GetFileUsage(ctx context.Context, objectID string) (*models.FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (models.FileUsageSummary, error)
}

// SHA256ValidityStore is the minimum storage surface needed by the SHA256 validity endpoint.
type SHA256ValidityStore interface {
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]models.InternalObject, error)
	ListS3Credentials(ctx context.Context) ([]models.S3Credential, error)
}

// MetricsStore is the minimum storage surface needed by the metrics API.
type MetricsStore interface {
	ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error)
	GetObject(ctx context.Context, id string) (*models.InternalObject, error)
	GetFileUsage(ctx context.Context, objectID string) (*models.FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (models.FileUsageSummary, error)
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
