# DrsServiceDrs

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**MaxBulkRequestLength** | **int32** | The max length the bulk request endpoints can handle (&gt;&#x3D; 1) before generating a 413 error e.g. how long can the arrays bulk_object_ids and bulk_object_access_ids be for this server. | 
**ObjectCount** | Pointer to **int32** | The total number of objects in this DRS service. | [optional] 
**TotalObjectSize** | Pointer to **int32** | The total size of all objects in this DRS service in bytes.  As a general best practice, file bytes are counted for each unique file and not cloud mirrors or other redundant copies. | [optional] 
**UploadRequestSupported** | Pointer to **bool** | Indicates whether this DRS server supports upload request operations via the &#x60;/upload-request&#x60; endpoint. If true, clients can request upload methods and credentials for uploading files. If false or missing, the server does not support upload request coordination. | [optional] [default to false]
**ObjectRegistrationSupported** | Pointer to **bool** | Indicates whether this DRS server supports object registration operations via the &#x60;/objects/register&#x60; endpoint. If true, clients can register uploaded files or existing data as DRS objects. If false or missing, the server does not support object registration. | [optional] [default to false]
**SupportedUploadMethods** | Pointer to **[]string** | List of upload methods supported by this DRS server. Only present when uploadRequestSupported is true. Clients can use this information to determine which upload methods are available before making upload requests. - **s3**: Direct S3 upload with temporary AWS credentials - **gs**: Google Cloud Storage upload with access tokens   - **https**: Presigned POST URL for HTTP uploads - **ftp**: File Transfer Protocol uploads - **sftp**: Secure File Transfer Protocol uploads - **gsiftp**: GridFTP secure file transfer - **globus**: Globus transfer service for high-performance data movement | [optional] 
**MaxUploadSize** | Pointer to **int64** | Maximum file size in bytes that can be uploaded via the upload endpoints. Only present when uploadRequestSupported is true. If not specified, there is no explicit size limit. | [optional] 
**MaxUploadRequestLength** | Pointer to **int32** | Maximum number of files that can be included in a single upload request. Only present when uploadRequestSupported is true. If not specified, defaults to the same value as maxBulkRequestLength. | [optional] 
**MaxRegisterRequestLength** | Pointer to **int32** | Maximum number of candidate objects that can be included in a single registration request. Only present when objectRegistrationSupported is true. If not specified, defaults to the same value as maxBulkRequestLength. | [optional] 
**ValidateUploadChecksums** | Pointer to **bool** | Indicates whether this DRS server validates uploaded file checksums against the provided metadata. If true, the server will verify that uploaded files match their declared checksums and may reject uploads with mismatches. If false or missing, the server does not perform checksum validation and relies on client-provided metadata. Only present when uploadRequestSupported or objectRegistrationSupported is true. | [optional] [default to false]
**ValidateUploadFileSizes** | Pointer to **bool** | Indicates whether this DRS server validates uploaded file sizes against the provided metadata. If true, the server will verify that uploaded files match their declared sizes and may reject uploads with mismatches. If false or missing, the server does not perform file size validation and relies on client-provided metadata. Only present when uploadRequestSupported or objectRegistrationSupported is true. | [optional] [default to false]
**RelatedFileStorageSupported** | Pointer to **bool** | Indicates whether this DRS server supports storing files from the same upload request under a common prefix or folder structure. If true, the server will organize related files together in storage, enabling bioinformatics workflows that expect co-located files (e.g., CRAM + CRAI, VCF + TBI). If false or missing, the server may distribute files across different storage locations or prefixes. Only present when uploadRequestSupported is true. This feature is particularly valuable for genomics tools like samtools that expect index files to be co-located with data files. | [optional] [default to false]
**DeleteSupported** | Pointer to **bool** | Indicates whether this DRS server supports delete operations via the delete endpoints. If true, clients can delete DRS objects using POST requests to &#x60;/objects/{object_id}/delete&#x60; and &#x60;/objects/delete&#x60;. If false or missing, the server does not support delete operations and will return 404 for delete endpoint requests. Like upload functionality, delete support is entirely optional and servers remain DRS compliant without it. | [optional] [default to false]
**MaxBulkDeleteLength** | Pointer to **int32** | Maximum number of objects that can be deleted in a single bulk delete request via &#x60;/objects/delete&#x60;. Only present when deleteSupported is true. If not specified when delete is supported, defaults to the same value as maxBulkRequestLength. Servers may enforce lower limits for delete operations compared to other bulk operations for safety reasons. | [optional] 
**DeleteStorageDataSupported** | Pointer to **bool** | Indicates whether this DRS server supports attempting to delete underlying storage data when clients request it. If true, the server will attempt to delete both metadata and storage files when &#x60;delete_storage_data: true&#x60; is specified in delete requests. If false or missing, the server only supports metadata deletion regardless of client request, preserving underlying storage data. Only present when deleteSupported is true. This is a capability flag indicating what the server can attempt, not a default behavior setting. Note: Storage deletion attempts may fail due to permissions, network issues, or storage service errors. | [optional] [default to false]
**AccessMethodUpdateSupported** | Pointer to **bool** | Indicates whether this DRS server supports updating access methods for existing objects. If true, clients can update access methods using &#x60;/objects/{object_id}/access-methods&#x60; and &#x60;/objects/access-methods&#x60; endpoints. If false or missing, the server does not support access method updates. | [optional] [default to false]
**MaxBulkAccessMethodUpdateLength** | Pointer to **int32** | Maximum number of objects that can be updated in a single bulk access method update request. Only present when accessMethodUpdateSupported is true. If not specified, defaults to maxBulkRequestLength. | [optional] 
**ValidateAccessMethodUpdates** | Pointer to **bool** | Indicates whether this DRS server validates new access methods by verifying they point to the same data. If true, the server will attempt to verify checksums/content before updating access methods. If false or missing, the server trusts client-provided access methods without validation. Only present when accessMethodUpdateSupported is true. | [optional] [default to false]

## Methods

### NewDrsServiceDrs

`func NewDrsServiceDrs(maxBulkRequestLength int32, ) *DrsServiceDrs`

NewDrsServiceDrs instantiates a new DrsServiceDrs object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDrsServiceDrsWithDefaults

`func NewDrsServiceDrsWithDefaults() *DrsServiceDrs`

NewDrsServiceDrsWithDefaults instantiates a new DrsServiceDrs object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetMaxBulkRequestLength

`func (o *DrsServiceDrs) GetMaxBulkRequestLength() int32`

GetMaxBulkRequestLength returns the MaxBulkRequestLength field if non-nil, zero value otherwise.

### GetMaxBulkRequestLengthOk

`func (o *DrsServiceDrs) GetMaxBulkRequestLengthOk() (*int32, bool)`

GetMaxBulkRequestLengthOk returns a tuple with the MaxBulkRequestLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxBulkRequestLength

`func (o *DrsServiceDrs) SetMaxBulkRequestLength(v int32)`

SetMaxBulkRequestLength sets MaxBulkRequestLength field to given value.


### GetObjectCount

`func (o *DrsServiceDrs) GetObjectCount() int32`

GetObjectCount returns the ObjectCount field if non-nil, zero value otherwise.

### GetObjectCountOk

`func (o *DrsServiceDrs) GetObjectCountOk() (*int32, bool)`

GetObjectCountOk returns a tuple with the ObjectCount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetObjectCount

`func (o *DrsServiceDrs) SetObjectCount(v int32)`

SetObjectCount sets ObjectCount field to given value.

### HasObjectCount

`func (o *DrsServiceDrs) HasObjectCount() bool`

HasObjectCount returns a boolean if a field has been set.

### GetTotalObjectSize

`func (o *DrsServiceDrs) GetTotalObjectSize() int32`

GetTotalObjectSize returns the TotalObjectSize field if non-nil, zero value otherwise.

### GetTotalObjectSizeOk

`func (o *DrsServiceDrs) GetTotalObjectSizeOk() (*int32, bool)`

GetTotalObjectSizeOk returns a tuple with the TotalObjectSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTotalObjectSize

`func (o *DrsServiceDrs) SetTotalObjectSize(v int32)`

SetTotalObjectSize sets TotalObjectSize field to given value.

### HasTotalObjectSize

`func (o *DrsServiceDrs) HasTotalObjectSize() bool`

HasTotalObjectSize returns a boolean if a field has been set.

### GetUploadRequestSupported

`func (o *DrsServiceDrs) GetUploadRequestSupported() bool`

GetUploadRequestSupported returns the UploadRequestSupported field if non-nil, zero value otherwise.

### GetUploadRequestSupportedOk

`func (o *DrsServiceDrs) GetUploadRequestSupportedOk() (*bool, bool)`

GetUploadRequestSupportedOk returns a tuple with the UploadRequestSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUploadRequestSupported

`func (o *DrsServiceDrs) SetUploadRequestSupported(v bool)`

SetUploadRequestSupported sets UploadRequestSupported field to given value.

### HasUploadRequestSupported

`func (o *DrsServiceDrs) HasUploadRequestSupported() bool`

HasUploadRequestSupported returns a boolean if a field has been set.

### GetObjectRegistrationSupported

`func (o *DrsServiceDrs) GetObjectRegistrationSupported() bool`

GetObjectRegistrationSupported returns the ObjectRegistrationSupported field if non-nil, zero value otherwise.

### GetObjectRegistrationSupportedOk

`func (o *DrsServiceDrs) GetObjectRegistrationSupportedOk() (*bool, bool)`

GetObjectRegistrationSupportedOk returns a tuple with the ObjectRegistrationSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetObjectRegistrationSupported

`func (o *DrsServiceDrs) SetObjectRegistrationSupported(v bool)`

SetObjectRegistrationSupported sets ObjectRegistrationSupported field to given value.

### HasObjectRegistrationSupported

`func (o *DrsServiceDrs) HasObjectRegistrationSupported() bool`

HasObjectRegistrationSupported returns a boolean if a field has been set.

### GetSupportedUploadMethods

`func (o *DrsServiceDrs) GetSupportedUploadMethods() []string`

GetSupportedUploadMethods returns the SupportedUploadMethods field if non-nil, zero value otherwise.

### GetSupportedUploadMethodsOk

`func (o *DrsServiceDrs) GetSupportedUploadMethodsOk() (*[]string, bool)`

GetSupportedUploadMethodsOk returns a tuple with the SupportedUploadMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSupportedUploadMethods

`func (o *DrsServiceDrs) SetSupportedUploadMethods(v []string)`

SetSupportedUploadMethods sets SupportedUploadMethods field to given value.

### HasSupportedUploadMethods

`func (o *DrsServiceDrs) HasSupportedUploadMethods() bool`

HasSupportedUploadMethods returns a boolean if a field has been set.

### GetMaxUploadSize

`func (o *DrsServiceDrs) GetMaxUploadSize() int64`

GetMaxUploadSize returns the MaxUploadSize field if non-nil, zero value otherwise.

### GetMaxUploadSizeOk

`func (o *DrsServiceDrs) GetMaxUploadSizeOk() (*int64, bool)`

GetMaxUploadSizeOk returns a tuple with the MaxUploadSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxUploadSize

`func (o *DrsServiceDrs) SetMaxUploadSize(v int64)`

SetMaxUploadSize sets MaxUploadSize field to given value.

### HasMaxUploadSize

`func (o *DrsServiceDrs) HasMaxUploadSize() bool`

HasMaxUploadSize returns a boolean if a field has been set.

### GetMaxUploadRequestLength

`func (o *DrsServiceDrs) GetMaxUploadRequestLength() int32`

GetMaxUploadRequestLength returns the MaxUploadRequestLength field if non-nil, zero value otherwise.

### GetMaxUploadRequestLengthOk

`func (o *DrsServiceDrs) GetMaxUploadRequestLengthOk() (*int32, bool)`

GetMaxUploadRequestLengthOk returns a tuple with the MaxUploadRequestLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxUploadRequestLength

`func (o *DrsServiceDrs) SetMaxUploadRequestLength(v int32)`

SetMaxUploadRequestLength sets MaxUploadRequestLength field to given value.

### HasMaxUploadRequestLength

`func (o *DrsServiceDrs) HasMaxUploadRequestLength() bool`

HasMaxUploadRequestLength returns a boolean if a field has been set.

### GetMaxRegisterRequestLength

`func (o *DrsServiceDrs) GetMaxRegisterRequestLength() int32`

GetMaxRegisterRequestLength returns the MaxRegisterRequestLength field if non-nil, zero value otherwise.

### GetMaxRegisterRequestLengthOk

`func (o *DrsServiceDrs) GetMaxRegisterRequestLengthOk() (*int32, bool)`

GetMaxRegisterRequestLengthOk returns a tuple with the MaxRegisterRequestLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxRegisterRequestLength

`func (o *DrsServiceDrs) SetMaxRegisterRequestLength(v int32)`

SetMaxRegisterRequestLength sets MaxRegisterRequestLength field to given value.

### HasMaxRegisterRequestLength

`func (o *DrsServiceDrs) HasMaxRegisterRequestLength() bool`

HasMaxRegisterRequestLength returns a boolean if a field has been set.

### GetValidateUploadChecksums

`func (o *DrsServiceDrs) GetValidateUploadChecksums() bool`

GetValidateUploadChecksums returns the ValidateUploadChecksums field if non-nil, zero value otherwise.

### GetValidateUploadChecksumsOk

`func (o *DrsServiceDrs) GetValidateUploadChecksumsOk() (*bool, bool)`

GetValidateUploadChecksumsOk returns a tuple with the ValidateUploadChecksums field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValidateUploadChecksums

`func (o *DrsServiceDrs) SetValidateUploadChecksums(v bool)`

SetValidateUploadChecksums sets ValidateUploadChecksums field to given value.

### HasValidateUploadChecksums

`func (o *DrsServiceDrs) HasValidateUploadChecksums() bool`

HasValidateUploadChecksums returns a boolean if a field has been set.

### GetValidateUploadFileSizes

`func (o *DrsServiceDrs) GetValidateUploadFileSizes() bool`

GetValidateUploadFileSizes returns the ValidateUploadFileSizes field if non-nil, zero value otherwise.

### GetValidateUploadFileSizesOk

`func (o *DrsServiceDrs) GetValidateUploadFileSizesOk() (*bool, bool)`

GetValidateUploadFileSizesOk returns a tuple with the ValidateUploadFileSizes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValidateUploadFileSizes

`func (o *DrsServiceDrs) SetValidateUploadFileSizes(v bool)`

SetValidateUploadFileSizes sets ValidateUploadFileSizes field to given value.

### HasValidateUploadFileSizes

`func (o *DrsServiceDrs) HasValidateUploadFileSizes() bool`

HasValidateUploadFileSizes returns a boolean if a field has been set.

### GetRelatedFileStorageSupported

`func (o *DrsServiceDrs) GetRelatedFileStorageSupported() bool`

GetRelatedFileStorageSupported returns the RelatedFileStorageSupported field if non-nil, zero value otherwise.

### GetRelatedFileStorageSupportedOk

`func (o *DrsServiceDrs) GetRelatedFileStorageSupportedOk() (*bool, bool)`

GetRelatedFileStorageSupportedOk returns a tuple with the RelatedFileStorageSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRelatedFileStorageSupported

`func (o *DrsServiceDrs) SetRelatedFileStorageSupported(v bool)`

SetRelatedFileStorageSupported sets RelatedFileStorageSupported field to given value.

### HasRelatedFileStorageSupported

`func (o *DrsServiceDrs) HasRelatedFileStorageSupported() bool`

HasRelatedFileStorageSupported returns a boolean if a field has been set.

### GetDeleteSupported

`func (o *DrsServiceDrs) GetDeleteSupported() bool`

GetDeleteSupported returns the DeleteSupported field if non-nil, zero value otherwise.

### GetDeleteSupportedOk

`func (o *DrsServiceDrs) GetDeleteSupportedOk() (*bool, bool)`

GetDeleteSupportedOk returns a tuple with the DeleteSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDeleteSupported

`func (o *DrsServiceDrs) SetDeleteSupported(v bool)`

SetDeleteSupported sets DeleteSupported field to given value.

### HasDeleteSupported

`func (o *DrsServiceDrs) HasDeleteSupported() bool`

HasDeleteSupported returns a boolean if a field has been set.

### GetMaxBulkDeleteLength

`func (o *DrsServiceDrs) GetMaxBulkDeleteLength() int32`

GetMaxBulkDeleteLength returns the MaxBulkDeleteLength field if non-nil, zero value otherwise.

### GetMaxBulkDeleteLengthOk

`func (o *DrsServiceDrs) GetMaxBulkDeleteLengthOk() (*int32, bool)`

GetMaxBulkDeleteLengthOk returns a tuple with the MaxBulkDeleteLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxBulkDeleteLength

`func (o *DrsServiceDrs) SetMaxBulkDeleteLength(v int32)`

SetMaxBulkDeleteLength sets MaxBulkDeleteLength field to given value.

### HasMaxBulkDeleteLength

`func (o *DrsServiceDrs) HasMaxBulkDeleteLength() bool`

HasMaxBulkDeleteLength returns a boolean if a field has been set.

### GetDeleteStorageDataSupported

`func (o *DrsServiceDrs) GetDeleteStorageDataSupported() bool`

GetDeleteStorageDataSupported returns the DeleteStorageDataSupported field if non-nil, zero value otherwise.

### GetDeleteStorageDataSupportedOk

`func (o *DrsServiceDrs) GetDeleteStorageDataSupportedOk() (*bool, bool)`

GetDeleteStorageDataSupportedOk returns a tuple with the DeleteStorageDataSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDeleteStorageDataSupported

`func (o *DrsServiceDrs) SetDeleteStorageDataSupported(v bool)`

SetDeleteStorageDataSupported sets DeleteStorageDataSupported field to given value.

### HasDeleteStorageDataSupported

`func (o *DrsServiceDrs) HasDeleteStorageDataSupported() bool`

HasDeleteStorageDataSupported returns a boolean if a field has been set.

### GetAccessMethodUpdateSupported

`func (o *DrsServiceDrs) GetAccessMethodUpdateSupported() bool`

GetAccessMethodUpdateSupported returns the AccessMethodUpdateSupported field if non-nil, zero value otherwise.

### GetAccessMethodUpdateSupportedOk

`func (o *DrsServiceDrs) GetAccessMethodUpdateSupportedOk() (*bool, bool)`

GetAccessMethodUpdateSupportedOk returns a tuple with the AccessMethodUpdateSupported field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessMethodUpdateSupported

`func (o *DrsServiceDrs) SetAccessMethodUpdateSupported(v bool)`

SetAccessMethodUpdateSupported sets AccessMethodUpdateSupported field to given value.

### HasAccessMethodUpdateSupported

`func (o *DrsServiceDrs) HasAccessMethodUpdateSupported() bool`

HasAccessMethodUpdateSupported returns a boolean if a field has been set.

### GetMaxBulkAccessMethodUpdateLength

`func (o *DrsServiceDrs) GetMaxBulkAccessMethodUpdateLength() int32`

GetMaxBulkAccessMethodUpdateLength returns the MaxBulkAccessMethodUpdateLength field if non-nil, zero value otherwise.

### GetMaxBulkAccessMethodUpdateLengthOk

`func (o *DrsServiceDrs) GetMaxBulkAccessMethodUpdateLengthOk() (*int32, bool)`

GetMaxBulkAccessMethodUpdateLengthOk returns a tuple with the MaxBulkAccessMethodUpdateLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxBulkAccessMethodUpdateLength

`func (o *DrsServiceDrs) SetMaxBulkAccessMethodUpdateLength(v int32)`

SetMaxBulkAccessMethodUpdateLength sets MaxBulkAccessMethodUpdateLength field to given value.

### HasMaxBulkAccessMethodUpdateLength

`func (o *DrsServiceDrs) HasMaxBulkAccessMethodUpdateLength() bool`

HasMaxBulkAccessMethodUpdateLength returns a boolean if a field has been set.

### GetValidateAccessMethodUpdates

`func (o *DrsServiceDrs) GetValidateAccessMethodUpdates() bool`

GetValidateAccessMethodUpdates returns the ValidateAccessMethodUpdates field if non-nil, zero value otherwise.

### GetValidateAccessMethodUpdatesOk

`func (o *DrsServiceDrs) GetValidateAccessMethodUpdatesOk() (*bool, bool)`

GetValidateAccessMethodUpdatesOk returns a tuple with the ValidateAccessMethodUpdates field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetValidateAccessMethodUpdates

`func (o *DrsServiceDrs) SetValidateAccessMethodUpdates(v bool)`

SetValidateAccessMethodUpdates sets ValidateAccessMethodUpdates field to given value.

### HasValidateAccessMethodUpdates

`func (o *DrsServiceDrs) HasValidateAccessMethodUpdates() bool`

HasValidateAccessMethodUpdates returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


