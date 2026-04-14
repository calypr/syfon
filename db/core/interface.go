package core

import (
	"context"
	"time"

	"github.com/calypr/syfon/apigen/drs"
)

// DatabaseInterface defines the methods required for a database backend
type DatabaseInterface interface {
	GetServiceInfo(ctx context.Context) (*drs.Service, error)
	GetObject(ctx context.Context, id string) (*InternalObject, error)
	DeleteObject(ctx context.Context, id string) error
	CreateObject(ctx context.Context, obj *InternalObject) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]InternalObject, error)
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]InternalObject, error)
	ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error)
	CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error
	ResolveObjectAlias(ctx context.Context, aliasID string) (string, error)

	// New Bulk Operations
	GetBulkObjects(ctx context.Context, ids []string) ([]InternalObject, error)
	BulkDeleteObjects(ctx context.Context, ids []string) error
	RegisterObjects(ctx context.Context, objects []InternalObject) error // Bulk Create

	// Access Methods
	UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error
	BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error // Map of ObjectID -> AccessMethods

	// S3 Credential Management
	GetS3Credential(ctx context.Context, bucket string) (*S3Credential, error)
	SaveS3Credential(ctx context.Context, cred *S3Credential) error
	DeleteS3Credential(ctx context.Context, bucket string) error
	ListS3Credentials(ctx context.Context) ([]S3Credential, error)
	CreateBucketScope(ctx context.Context, scope *BucketScope) error
	GetBucketScope(ctx context.Context, organization, projectID string) (*BucketScope, error)
	ListBucketScopes(ctx context.Context) ([]BucketScope, error)

	// LFS pending metadata lifecycle.
	SavePendingLFSMeta(ctx context.Context, entries []PendingLFSMeta) error
	GetPendingLFSMeta(ctx context.Context, oid string) (*PendingLFSMeta, error)
	PopPendingLFSMeta(ctx context.Context, oid string) (*PendingLFSMeta, error)

	// File usage metrics lifecycle.
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
	GetFileUsage(ctx context.Context, objectID string) (*FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (FileUsageSummary, error)
}
