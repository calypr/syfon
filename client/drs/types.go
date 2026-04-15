package drs

import (
	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/client/pkg/hash"
)

type ChecksumType = string
type Checksum = drsapi.Checksum
type HashInfo = hash.HashInfo

type AccessURL = drsapi.AccessURL
type Authorizations = drsapi.Authorizations
type AccessMethod = drsapi.AccessMethod

type Contents = drsapi.ContentsObject

type DRSPage = drsapi.DRSPage

type DRSObjectResult struct {
	Object *DRSObject
	Error  error
}

type DRSObject = drsapi.DrsObject

type DRSObjectCandidate = drsapi.DrsObjectCandidate
type RegisterObjectsRequest = drsapi.RegisterObjectsBody

// SyncCandidate represents a local file record to be synchronized with DRS.
// It is the generic version of the git-drs LfsFileInfo.
type SyncCandidate struct {
	Name string
	Size int64
	Oid  string
	// The local filesystem path to the payload.
	Path      string
	IsPointer bool
}

// UploadConfig controls the behavior of high-level file upload operations.
type UploadConfig struct {
	// Threshold in bytes for switching from single-part to multipart upload.
	MultiPartThreshold int64
	// Concurrency for small file uploads and multipart part uploads.
	UploadConcurrency int
	// Whether to overwrite existing metadata if checksums match but IDs differ.
	Upsert bool
}
