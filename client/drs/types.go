package drs

import (
	"github.com/calypr/syfon/apitypes"
	"github.com/calypr/syfon/client/pkg/hash"
)

type ChecksumType = string
type Checksum = apitypes.Checksum
type HashInfo = hash.HashInfo

type AccessURL = apitypes.AccessMethodAccessURL
type Authorizations = apitypes.AccessMethodAuthorizations
type AccessMethod = apitypes.AccessMethod

type Contents = apitypes.ContentsObject

type DRSPage = apitypes.DRSPage

type DRSObjectResult struct {
	Object *DRSObject
	Error  error
}

type DRSObject = apitypes.DRSObject

type DRSObjectCandidate = apitypes.DRSObjectCandidate
type RegisterObjectsRequest = apitypes.RegisterObjectsRequest

type InternalRecordRequest = apitypes.InternalRecordRequest
type InternalRecordResponse = apitypes.InternalRecordResponse
type ListRecordsResponse = apitypes.ListRecordsResponse
type BulkHashesRequest = apitypes.BulkHashesRequest
type BulkCreateRequest = apitypes.BulkCreateRequest
type MultipartPart = apitypes.MultipartPart

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
