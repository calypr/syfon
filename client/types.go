package client

import (
	bucketapi "github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
)

// Canonical API model aliases.
type Checksum = drsapi.Checksum
type AccessMethodAuthorizations = drsapi.AccessMethodAuthorizations
type AccessMethodAccessURL = drsapi.AccessMethodAccessUrl
type AccessMethod = drsapi.AccessMethod
type ContentsObject = drsapi.ContentsObject
type DRSObject = drsapi.DrsObject
type DRSPage = drsapi.DrsPage
type DRSObjectCandidate = drsapi.DrsObjectCandidate
type RegisterObjectsRequest = drsapi.RegisterObjectsRequest
type RegisterObjectsResponse = drsapi.RegisterObjects201Response
type InternalRecordRequest = internalapi.InternalRecord
type InternalRecord = internalapi.InternalRecordResponse
type ListRecordsResponse = internalapi.ListRecordsResponse
type BulkCreateRequest = internalapi.BulkCreateRequest
type BulkHashesRequest = internalapi.BulkHashesRequest
type BulkSHA256ValidityRequest = internalapi.BulkSHA256ValidityRequest
type DeleteByQueryResponse = internalapi.DeleteByQueryResponse
type MultipartPart = internalapi.InternalMultipartPart
type PutBucketRequest = bucketapi.PutBucketRequest
type BucketScopeRequest = bucketapi.AddBucketScopeRequest
type BucketsResponse = bucketapi.BucketsResponse
type UploadBulkRequest = internalapi.InternalUploadBulkRequest
type UploadBulkItem = internalapi.InternalUploadBulkItem
type UploadBulkResponse = internalapi.InternalUploadBulkResponse
type UploadBulkResult = internalapi.InternalUploadBulkResult
type SignedURL = internalapi.InternalSignedURL
type UploadBlankRequest = internalapi.InternalUploadBlankRequest
type UploadBlankResponse = internalapi.InternalUploadBlankResponse
type MultipartInitRequest = internalapi.InternalMultipartInitRequest
type MultipartInitResponse = internalapi.InternalMultipartInitResponse
type MultipartUploadRequest = internalapi.InternalMultipartUploadRequest
type MultipartUploadResponse = internalapi.InternalMultipartUploadResponse
type MultipartCompleteRequest = internalapi.InternalMultipartCompleteRequest

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
