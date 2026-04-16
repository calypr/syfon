package client

import (
	"github.com/calypr/syfon/apitypes"
)

// Canonical API model aliases now sourced from the unified apitypes package.
type Checksum = apitypes.Checksum
type AccessMethod = apitypes.AccessMethod
type AccessMethodAuthorizationsSupportedTypes = apitypes.AccessMethodAuthorizationsSupportedTypes
type AccessMethodAccessURL = apitypes.AccessMethodAccessURL
type ContentsObject = apitypes.ContentsObject
type DRSObject = apitypes.DRSObject
type DRSPage = apitypes.DRSPage
type DRSObjectCandidate = apitypes.DRSObjectCandidate
type RegisterObjectsRequest = apitypes.RegisterObjectsRequest
type RegisterObjectsResponse = apitypes.RegisterObjectsResponse
type InternalRecordRequest = apitypes.InternalRecordRequest
type InternalRecordResponse = apitypes.InternalRecordResponse
type ListRecordsResponse = apitypes.ListRecordsResponse
type BulkCreateRequest = apitypes.BulkCreateRequest
type BulkHashesRequest = apitypes.BulkHashesRequest
type BulkSHA256ValidityRequest = apitypes.BulkSHA256ValidityRequest
type DeleteByQueryResponse = apitypes.DeleteByQueryResponse
type MultipartPart = apitypes.MultipartPart
type PutBucketRequest = apitypes.PutBucketRequest
type BucketScopeRequest = apitypes.BucketScopeRequest
type BucketsResponse = apitypes.BucketsResponse
type UploadBulkRequest = apitypes.UploadBulkRequest
type UploadBulkItem = apitypes.UploadBulkItem
type UploadBulkResponse = apitypes.UploadBulkResponse
type UploadBulkResult = apitypes.UploadBulkResult
type SignedURL = apitypes.SignedURL
type UploadBlankRequest = apitypes.UploadBlankRequest
type UploadBlankResponse = apitypes.UploadBlankResponse
type MultipartInitRequest = apitypes.MultipartInitRequest
type MultipartInitResponse = apitypes.MultipartInitResponse
type MultipartUploadRequest = apitypes.MultipartUploadRequest
type MultipartUploadResponse = apitypes.MultipartUploadResponse
type MultipartCompleteRequest = apitypes.MultipartCompleteRequest

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
