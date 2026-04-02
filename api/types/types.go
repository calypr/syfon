package types

import (
	bucketapi "github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
	metricsapi "github.com/calypr/syfon/apigen/metricsapi"
)

// GA4GH DRS models.
type Checksum = drsapi.Checksum
type AccessMethod = drsapi.AccessMethod
type AccessMethodAuthorizations = drsapi.AccessMethodAuthorizations
type AccessMethodAccessURL = drsapi.AccessMethodAccessUrl
type AccessURL = drsapi.AccessUrl
type ContentsObject = drsapi.ContentsObject
type DRSObject = drsapi.DrsObject
type DRSPage = drsapi.DrsPage
type DRSObjectCandidate = drsapi.DrsObjectCandidate
type RegisterObjectsRequest = drsapi.RegisterObjectsRequest
type RegisterObjectsResponse = drsapi.RegisterObjects201Response

// Internal API models.
type InternalRecord = internalapi.InternalRecordResponse
type InternalRecordRequest = internalapi.InternalRecord
type ListRecordsResponse = internalapi.ListRecordsResponse
type BulkCreateRequest = internalapi.BulkCreateRequest
type BulkHashesRequest = internalapi.BulkHashesRequest
type BulkSHA256ValidityRequest = internalapi.BulkSHA256ValidityRequest
type DeleteByQueryResponse = internalapi.DeleteByQueryResponse
type UploadBlankRequest = internalapi.InternalUploadBlankRequest
type UploadBlankResponse = internalapi.InternalUploadBlankResponse
type SignedURLResponse = internalapi.InternalSignedURL
type UploadBulkRequest = internalapi.InternalUploadBulkRequest
type UploadBulkItem = internalapi.InternalUploadBulkItem
type UploadBulkResponse = internalapi.InternalUploadBulkResponse
type UploadBulkResult = internalapi.InternalUploadBulkResult
type MultipartInitRequest = internalapi.InternalMultipartInitRequest
type MultipartInitResponse = internalapi.InternalMultipartInitResponse
type MultipartUploadRequest = internalapi.InternalMultipartUploadRequest
type MultipartUploadResponse = internalapi.InternalMultipartUploadResponse
type MultipartPart = internalapi.InternalMultipartPart
type MultipartCompleteRequest = internalapi.InternalMultipartCompleteRequest

// Bucket API models.
type PutBucketRequest = bucketapi.PutBucketRequest
type AddBucketScopeRequest = bucketapi.AddBucketScopeRequest
type BucketsResponse = bucketapi.BucketsResponse
type BucketMetadata = bucketapi.BucketMetadata

// Metrics API models.
type FileUsage = metricsapi.FileUsage
type FileUsageSummary = metricsapi.FileUsageSummary
type MetricsListResponse = metricsapi.MetricsListResponse
