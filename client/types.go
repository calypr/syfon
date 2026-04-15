package client

import (
	"github.com/calypr/syfon/api/types"
)

// Canonical API model aliases now sourced from the unified api/types package.
type Checksum = types.Checksum
type AccessMethod = types.AccessMethod
type AccessMethodAuthorizationsSupportedTypes = types.AccessMethodAuthorizationsSupportedTypes
type AccessMethodAccessURL = types.AccessMethodAccessURL
type ContentsObject = types.ContentsObject
type DRSObject = types.DRSObject
type DRSPage = types.DRSPage
type DRSObjectCandidate = types.DRSObjectCandidate
type RegisterObjectsRequest = types.RegisterObjectsRequest
type RegisterObjectsResponse = types.RegisterObjectsResponse
type InternalRecordRequest = types.InternalRecordRequest
type InternalRecordResponse = types.InternalRecordResponse
type ListRecordsResponse = types.ListRecordsResponse
type BulkCreateRequest = types.BulkCreateRequest
type BulkHashesRequest = types.BulkHashesRequest
type BulkSHA256ValidityRequest = types.BulkSHA256ValidityRequest
type DeleteByQueryResponse = types.DeleteByQueryResponse
type MultipartPart = types.MultipartPart
type PutBucketRequest = types.PutBucketRequest
type BucketScopeRequest = types.BucketScopeRequest
type BucketsResponse = types.BucketsResponse
type UploadBulkRequest = types.UploadBulkRequest
type UploadBulkItem = types.UploadBulkItem
type UploadBulkResponse = types.UploadBulkResponse
type UploadBulkResult = types.UploadBulkResult
type SignedURL = types.SignedURL
type UploadBlankRequest = types.UploadBlankRequest
type UploadBlankResponse = types.UploadBlankResponse
type MultipartInitRequest = types.MultipartInitRequest
type MultipartInitResponse = types.MultipartInitResponse
type MultipartUploadRequest = types.MultipartUploadRequest
type MultipartUploadResponse = types.MultipartUploadResponse
type MultipartCompleteRequest = types.MultipartCompleteRequest

type DeleteByQueryOptions struct {
	Authz        string
	Organization string
	ProjectID    string
	Hash         string
	HashType     string
}

type ListRecordsOptions struct {
	Hash         string
	URL          string
	Authz        string
	Organization string
	ProjectID    string
	Limit        int
	Page         int
}

type UploadURLRequest struct {
	FileID    string
	Bucket    string
	FileName  string
	ExpiresIn int
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
