# UploadMethod

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | **string** | Type of upload method. Implementations MAY support any subset of these types. The &#39;https&#39; type can be used to return a presigned POST URL and is expected to be the most common implementation for typical file uploads. This method provides a simple HTTP POST interface that works with standard web clients. The &#39;s3&#39; type is primarily intended to support uploads of large files that want to take advantage of multipart uploads and automatic retries implemented in AWS libraries. This method provides direct access to S3-specific upload capabilities. Other common implementations include &#39;gs&#39; for Google Cloud Storage and &#39;sftp&#39; for secure FTP uploads. | 
**AccessUrl** | [**UploadMethodAccessUrl**](UploadMethodAccessUrl.md) |  | 
**Region** | Pointer to **string** | Cloud region for the upload location. Optional for non-cloud storage types. | [optional] 
**UploadDetails** | Pointer to **map[string]interface{}** | A dictionary of upload-specific configuration details that vary by upload method type. The contents and structure depend on the specific upload method being used. | [optional] 

## Methods

### NewUploadMethod

`func NewUploadMethod(type_ string, accessUrl UploadMethodAccessUrl, ) *UploadMethod`

NewUploadMethod instantiates a new UploadMethod object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUploadMethodWithDefaults

`func NewUploadMethodWithDefaults() *UploadMethod`

NewUploadMethodWithDefaults instantiates a new UploadMethod object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetType

`func (o *UploadMethod) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *UploadMethod) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *UploadMethod) SetType(v string)`

SetType sets Type field to given value.


### GetAccessUrl

`func (o *UploadMethod) GetAccessUrl() UploadMethodAccessUrl`

GetAccessUrl returns the AccessUrl field if non-nil, zero value otherwise.

### GetAccessUrlOk

`func (o *UploadMethod) GetAccessUrlOk() (*UploadMethodAccessUrl, bool)`

GetAccessUrlOk returns a tuple with the AccessUrl field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessUrl

`func (o *UploadMethod) SetAccessUrl(v UploadMethodAccessUrl)`

SetAccessUrl sets AccessUrl field to given value.


### GetRegion

`func (o *UploadMethod) GetRegion() string`

GetRegion returns the Region field if non-nil, zero value otherwise.

### GetRegionOk

`func (o *UploadMethod) GetRegionOk() (*string, bool)`

GetRegionOk returns a tuple with the Region field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRegion

`func (o *UploadMethod) SetRegion(v string)`

SetRegion sets Region field to given value.

### HasRegion

`func (o *UploadMethod) HasRegion() bool`

HasRegion returns a boolean if a field has been set.

### GetUploadDetails

`func (o *UploadMethod) GetUploadDetails() map[string]interface{}`

GetUploadDetails returns the UploadDetails field if non-nil, zero value otherwise.

### GetUploadDetailsOk

`func (o *UploadMethod) GetUploadDetailsOk() (*map[string]interface{}, bool)`

GetUploadDetailsOk returns a tuple with the UploadDetails field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUploadDetails

`func (o *UploadMethod) SetUploadDetails(v map[string]interface{})`

SetUploadDetails sets UploadDetails field to given value.

### HasUploadDetails

`func (o *UploadMethod) HasUploadDetails() bool`

HasUploadDetails returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


