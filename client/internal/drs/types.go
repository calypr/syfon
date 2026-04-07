package drs

import (
	"github.com/calypr/syfon/client/pkg/hash"
	syclient "github.com/calypr/syfon/client"
)

type ChecksumType = string
type Checksum = syclient.Checksum
type HashInfo = hash.HashInfo

type AccessURL = syclient.AccessMethodAccessURL
type Authorizations = syclient.AccessMethodAuthorizations
type AccessMethod = syclient.AccessMethod

type Contents = syclient.ContentsObject

type DRSPage = syclient.DRSPage

type DRSObjectResult struct {
	Object *DRSObject
	Error  error
}

type DRSObject = syclient.DRSObject

type DRSObjectCandidate = syclient.DRSObjectCandidate
type RegisterObjectsRequest = syclient.RegisterObjectsRequest

// SyncCandidate represents a local file record to be synchronized with DRS.
// It is the generic version of the git-drs LfsFileInfo.
type SyncCandidate struct {
	Name      string
	Size      int64
	Oid       string
	// The local filesystem path to the payload.
	Path      string
	IsPointer bool
}

// UploadConfig controls the behavior of high-level file upload operations.
type UploadConfig struct {
	// Threshold in bytes for switching from single-part to multipart upload.
	MultiPartThreshold int64
	// Concurrency for small file uploads and multipart part uploads.
	UploadConcurrency  int
	// Whether to overwrite existing metadata if checksums match but IDs differ.
	Upsert             bool
}
