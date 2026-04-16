package core

import (
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

// DrsObjectRecord represents the internal database record for a DRS Object
type DrsObjectRecord struct {
	ID          string    `db:"id"`
	Description string    `db:"description"`
	CreatedTime time.Time `db:"created_time"`
	UpdatedTime time.Time `db:"updated_time"`
	Size        int64     `db:"size"`
	Version     string    `db:"version"`
	Name        string    `db:"name"`
	MimeType    string    `db:"mime_type"`
}

// InternalObject is the internal DRS domain model used by the fast/internal API
// and storage layer. The official GA4GH DRS schema object lives in `drs.DrsObject`.
type InternalObject struct {
	drs.DrsObject
	Authorizations []string
}

// DrsObjectWithAuthz is retained as a compatibility alias while code migrates to InternalObject.
type DrsObjectWithAuthz = InternalObject

func (o InternalObject) External() drs.DrsObject {
	return o.DrsObject
}

// DrsObjectAccessMethod represents the internal database record for a DRS Access Method (URL)
type DrsObjectAccessMethod struct {
	ObjectID string `db:"object_id"`
	URL      string `db:"url"`
	Type     string `db:"type"` // e.g., "s3"
}

// DrsObjectAuthz represents the internal database record for DRS RBAC
type DrsObjectAuthz struct {
	ObjectID string `db:"object_id"`
	Resource string `db:"resource"`
}

// DrsObjectChecksum represents the internal database record for DRS Checksums
type DrsObjectChecksum struct {
	ObjectID string `db:"object_id"`
	Type     string `db:"type"`
	Checksum string `db:"checksum"`
}

// S3Credential represents the 's3_credential' table
type S3Credential struct {
	Bucket    string `db:"bucket"`
	Provider  string `db:"provider"`
	Region    string `db:"region"`
	AccessKey string `db:"access_key"`
	SecretKey string `db:"secret_key"`
	Endpoint  string `db:"endpoint"`
}

type BucketScope struct {
	Organization string `db:"organization"`
	ProjectID    string `db:"project_id"`
	Bucket       string `db:"bucket"`
	PathPrefix   string `db:"path_prefix"`
}

// PendingLFSMeta stores a staged LFS metadata packet keyed by object checksum.
// It is submitted before transfer and consumed at verify-time.
type PendingLFSMeta struct {
	OID       string
	Candidate drs.DrsObjectCandidate
	CreatedAt time.Time
	ExpiresAt time.Time
}

// FileUsage captures per-object transfer activity that can drive lifecycle policies.
type FileUsage struct {
	ObjectID         string
	Name             string
	Size             int64
	UploadCount      int64
	DownloadCount    int64
	LastUploadTime   *time.Time
	LastDownloadTime *time.Time
	LastAccessTime   *time.Time
}

// FileUsageSummary provides aggregate transfer insights.
type FileUsageSummary struct {
	TotalFiles        int64
	TotalUploads      int64
	TotalDownloads    int64
	InactiveFileCount int64
}
