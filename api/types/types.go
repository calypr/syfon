package types

import (
	bucketapi "github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
)

// --- DRS Models ---

type Checksum = drsapi.Checksum
type AccessMethodAuthorizations = drsapi.AccessMethodAuthorizations
type AccessMethodAuthorizationsSupportedTypes = drsapi.AccessMethodAuthorizationsSupportedTypes
type AccessMethodAccessURL = drsapi.AccessURL
type AccessMethod = drsapi.AccessMethod
type ContentsObject = drsapi.ContentsObject
type DRSObject = drsapi.DrsObject
type DRSPage = drsapi.DRSPage
type DRSObjectCandidate = drsapi.DrsObjectCandidate
type RegisterObjectsRequest = drsapi.RegisterObjectsJSONBody
type RegisterObjectsResponse = drsapi.RegisterObjects201JSONResponse

// --- Internal Record Models ---

type InternalRecordRequest = internalapi.InternalRecord
type InternalRecordResponse = internalapi.InternalRecordResponse
type ListRecordsResponse = internalapi.ListRecordsResponse
type BulkCreateRequest = internalapi.BulkCreateRequest
type BulkHashesRequest = internalapi.BulkHashesRequest
type BulkSHA256ValidityRequest = internalapi.BulkSHA256ValidityRequest
type DeleteByQueryResponse = internalapi.DeleteByQueryResponse
type MultipartPart = internalapi.InternalMultipartPart

// --- Bucket Models ---

type PutBucketRequest = bucketapi.PutBucketRequest
type AddBucketScopeRequest = bucketapi.AddBucketScopeRequest
type BucketScopeRequest = bucketapi.AddBucketScopeRequest
type BucketsResponse = bucketapi.BucketsResponse

// --- Upload & Multipart Models ---

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
