package client

import (
	apitypes "github.com/calypr/syfon/api/types"
)

// Canonical API model aliases.
type Checksum = apitypes.Checksum
type AccessMethodAuthorizations = apitypes.AccessMethodAuthorizations
type AccessMethodAccessURL = apitypes.AccessMethodAccessURL
type AccessMethod = apitypes.AccessMethod
type ContentsObject = apitypes.ContentsObject
type DRSObject = apitypes.DRSObject
type DRSPage = apitypes.DRSPage
type DRSObjectCandidate = apitypes.DRSObjectCandidate
type RegisterObjectsRequest = apitypes.RegisterObjectsRequest
type RegisterObjectsResponse = apitypes.RegisterObjectsResponse
type InternalRecord = apitypes.InternalRecord
type ListRecordsResponse = apitypes.ListRecordsResponse
type BulkCreateRequest = apitypes.BulkCreateRequest
type BulkHashesRequest = apitypes.BulkHashesRequest
type BulkSHA256ValidityRequest = apitypes.BulkSHA256ValidityRequest
type DeleteByQueryResponse = apitypes.DeleteByQueryResponse
type MultipartPart = apitypes.MultipartPart
type PutBucketRequest = apitypes.PutBucketRequest
type BucketScopeRequest = apitypes.AddBucketScopeRequest
type BucketsResponse = apitypes.BucketsResponse
type UploadBulkRequest = apitypes.UploadBulkRequest
type UploadBulkItem = apitypes.UploadBulkItem
type UploadBulkResponse = apitypes.UploadBulkResponse
type UploadBulkResult = apitypes.UploadBulkResult

// SignedURL is the CLI/runtime-friendly response for signed URL endpoints.
// OpenAPI currently models these slightly differently across routes.
type SignedURL struct {
	GUID   string `json:"guid,omitempty"`
	URL    string `json:"url,omitempty"`
	Bucket string `json:"bucket,omitempty"`
}

type DeleteByQueryOptions struct {
	Authz        string
	Organization string
	ProjectID    string
	Hash         string
	HashType     string
}

type ListRecordsOptions struct {
	Hash         string
	Authz        string
	Organization string
	ProjectID    string
	Limit        int
	Page         int
}

type UploadBlankRequest struct {
	GUID  string   `json:"guid"`
	Authz []string `json:"authz,omitempty"`
}

type UploadURLRequest struct {
	FileID    string
	Bucket    string
	FileName  string
	ExpiresIn int
}

type MultipartInitRequest struct {
	GUID     string `json:"guid,omitempty"`
	FileName string `json:"file_name,omitempty"`
	Bucket   string `json:"bucket,omitempty"`
}

type MultipartInitResponse struct {
	GUID     string `json:"guid,omitempty"`
	UploadID string `json:"uploadId,omitempty"`
}

type MultipartUploadRequest struct {
	Key        string `json:"key"`
	UploadID   string `json:"uploadId"`
	PartNumber int32  `json:"partNumber"`
	Bucket     string `json:"bucket,omitempty"`
}

type MultipartUploadResponse struct {
	PresignedURL string `json:"presigned_url,omitempty"`
}

type MultipartCompleteRequest struct {
	Key      string          `json:"key"`
	UploadID string          `json:"uploadId"`
	Bucket   string          `json:"bucket,omitempty"`
	Parts    []MultipartPart `json:"parts"`
}

type MetricsFilesOptions struct {
	Limit        int
	Offset       int
	InactiveDays int
	Organization string
	ProjectID    string
}

type MetricsSummaryOptions struct {
	InactiveDays int
	Organization string
	ProjectID    string
}
