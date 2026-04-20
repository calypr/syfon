package models

import (
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

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

// FileUsageSummary aggregates transfer activity for a set of objects.
type FileUsageSummary struct {
	TotalFiles        int64
	TotalUploads      int64
	TotalDownloads    int64
	InactiveFileCount int64
}

// DrsObjectRecord mirrors the subset of drs_object columns returned by storage queries.
type DrsObjectRecord struct {
	ID          string
	Size        int64
	CreatedTime time.Time
	UpdatedTime time.Time
	Name        string
	Version     string
	Description string
}
