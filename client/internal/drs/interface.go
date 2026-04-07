package drs

import (
	"context"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/transfer"
)

// MetadataManager defines the read-only resolution operations.
type MetadataManager interface {
	GetObject(ctx context.Context, id string) (*DRSObject, error)
	GetObjectByHash(ctx context.Context, ck *hash.Checksum) ([]DRSObject, error)
	BatchGetObjectsByHash(ctx context.Context, hashes []string) (map[string][]DRSObject, error)
	ListObjects(ctx context.Context) (chan DRSObjectResult, error)
	ListObjectsByProject(ctx context.Context, projectId string) (chan DRSObjectResult, error)
	GetProjectSample(ctx context.Context, projectId string, limit int) ([]DRSObject, error)
}

// MutableMetadataManager handles record creation and updates.
// Segregated to maintain a pure Resolution Layer while supporting Admin operations.
type MutableMetadataManager interface {
	RegisterRecord(ctx context.Context, record *DRSObject) (*DRSObject, error)
	RegisterRecords(ctx context.Context, records []*DRSObject) ([]*DRSObject, error)
	UpdateRecord(ctx context.Context, updateInfo *DRSObject, did string) (*DRSObject, error)
	DeleteRecordsByProject(ctx context.Context, projectId string) error
	DeleteRecordByOID(ctx context.Context, oid string) error
	DeleteRecordsByChecksums(ctx context.Context, checksums []*hash.Checksum) (int, error)
	DeleteRecord(ctx context.Context, did string) error

	// Registration Helpers (macros)
	RegisterFile(ctx context.Context, oid, path string) (*DRSObject, error)
	AddURL(ctx context.Context, blobURL, sha256 string, opts ...AddURLOption) (*DRSObject, error)
	UpsertRecord(ctx context.Context, url string, sha256 string, fileSize int64, projectId string) (*DRSObject, error)

	// URL Management
	ResolveUploadURL(ctx context.Context, guid, filename string, metadata common.FileMetadata, bucket string) (string, error)
	ResolveUploadURLs(ctx context.Context, requests []common.UploadURLResolveRequest) ([]common.UploadURLResolveResponse, error)
}

// URLSigner handles generating signed URLs for access.
type URLSigner interface {
	GetDownloadURL(ctx context.Context, id string, accessID string) (*AccessURL, error)
	GetDownloadPartURL(ctx context.Context, id string, start, end int64) (*transfer.SignedURL, error)
	GetUploadURL(ctx context.Context, id string) (*AccessURL, error)
}

// Resolver handles logical-to-physical mapping.
type Resolver interface {
	// Resolve translates a GUID into a physical transfer specification Across S3, GCS, and Azure.
	Resolve(ctx context.Context, id string) (*transfer.ResolvedObject, error)
}

// Client facilitates high-level Data Repository Service operations.
// It translates logical GUIDs into actionable transfer plans by embedding
// all granular resolution and transport capabilities.
type Client interface {
	MetadataManager
	MutableMetadataManager
	URLSigner
	Resolver
	transfer.Backend
	transfer.Provider

	// Fluent context helpers.
	WithProject(projectId string) Client
	WithOrganization(orgName string) Client
	WithBucket(bucketName string) Client

	GetProjectId() string
	GetBucketName() string
	GetOrganization() string
}


// AddURLOption defines functional options for AddURL.
type AddURLOption func(o *DRSObject)
