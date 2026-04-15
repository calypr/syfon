package drs

import (
	"github.com/calypr/syfon/api/types"
	"github.com/calypr/syfon/client/pkg/hash"
)

type ChecksumType = string
type Checksum = types.Checksum
type HashInfo = hash.HashInfo

type AccessURL = types.AccessMethodAccessURL
type Authorizations = types.AccessMethodAuthorizations
type AccessMethod = types.AccessMethod

type Contents = types.ContentsObject

type DRSPage = types.DRSPage

type DRSObjectResult struct {
	Object *DRSObject
	Error  error
}

type DRSObject = types.DRSObject

type DRSObjectCandidate = types.DRSObjectCandidate
type RegisterObjectsRequest = types.RegisterObjectsRequest

type InternalRecordRequest = types.InternalRecordRequest
type InternalRecordResponse = types.InternalRecordResponse
type ListRecordsResponse = types.ListRecordsResponse
type BulkHashesRequest = types.BulkHashesRequest
type BulkCreateRequest = types.BulkCreateRequest
type MultipartPart = types.MultipartPart

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
