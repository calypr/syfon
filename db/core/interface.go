package core

import (
	"context"

	"github.com/calypr/drs-server/apigen/drs"
)

// DatabaseInterface defines the methods required for a database backend
type DatabaseInterface interface {
	GetServiceInfo(ctx context.Context) (*drs.Service, error)
	GetObject(ctx context.Context, id string) (*drs.DrsObject, error)
	DeleteObject(ctx context.Context, id string) error
	CreateObject(ctx context.Context, obj *drs.DrsObject, authz []string) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error)
	GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]drs.DrsObject, error)

	// New Bulk Operations
	GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error)
	BulkDeleteObjects(ctx context.Context, ids []string) error
	RegisterObjects(ctx context.Context, objects []DrsObjectWithAuthz) error // Bulk Create

	// Access Methods
	UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error
	BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error // Map of ObjectID -> AccessMethods

	// S3 Credential Management
	GetS3Credential(ctx context.Context, bucket string) (*S3Credential, error)
	SaveS3Credential(ctx context.Context, cred *S3Credential) error
	DeleteS3Credential(ctx context.Context, bucket string) error
	ListS3Credentials(ctx context.Context) ([]S3Credential, error)
}
