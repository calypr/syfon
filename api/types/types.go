package types

import (
	bucketapi "github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
)

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

type InternalRecord = internalapi.InternalRecord
type InternalRecordResponse = internalapi.InternalRecordResponse
type ListRecordsResponse = internalapi.ListRecordsResponse
type BulkCreateRequest = internalapi.BulkCreateRequest
type BulkHashesRequest = internalapi.BulkHashesRequest
type BulkSHA256ValidityRequest = internalapi.BulkSHA256ValidityRequest
type DeleteByQueryResponse = internalapi.DeleteByQueryResponse
type MultipartPart = internalapi.InternalMultipartPart

type PutBucketRequest = bucketapi.PutBucketRequest
type AddBucketScopeRequest = bucketapi.AddBucketScopeRequest
type BucketsResponse = bucketapi.BucketsResponse

type UploadBulkRequest = internalapi.InternalUploadBulkRequest
type UploadBulkItem = internalapi.InternalUploadBulkItem
type UploadBulkResponse = internalapi.InternalUploadBulkResponse
type UploadBulkResult = internalapi.InternalUploadBulkResult
