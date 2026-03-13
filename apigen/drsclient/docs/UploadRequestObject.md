# UploadRequestObject

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | The name of the file to upload | 
**Size** | **int64** | Size of the file in bytes | 
**MimeType** | **string** | MIME type of the file | 
**Checksums** | [**[]Checksum**](Checksum.md) | Array of checksums for file integrity verification | 
**Description** | Pointer to **string** | Optional description of the file | [optional] 
**Aliases** | Pointer to **[]string** | Optional array of alternative names for the file | [optional] 

## Methods

### NewUploadRequestObject

`func NewUploadRequestObject(name string, size int64, mimeType string, checksums []Checksum, ) *UploadRequestObject`

NewUploadRequestObject instantiates a new UploadRequestObject object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUploadRequestObjectWithDefaults

`func NewUploadRequestObjectWithDefaults() *UploadRequestObject`

NewUploadRequestObjectWithDefaults instantiates a new UploadRequestObject object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *UploadRequestObject) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *UploadRequestObject) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *UploadRequestObject) SetName(v string)`

SetName sets Name field to given value.


### GetSize

`func (o *UploadRequestObject) GetSize() int64`

GetSize returns the Size field if non-nil, zero value otherwise.

### GetSizeOk

`func (o *UploadRequestObject) GetSizeOk() (*int64, bool)`

GetSizeOk returns a tuple with the Size field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSize

`func (o *UploadRequestObject) SetSize(v int64)`

SetSize sets Size field to given value.


### GetMimeType

`func (o *UploadRequestObject) GetMimeType() string`

GetMimeType returns the MimeType field if non-nil, zero value otherwise.

### GetMimeTypeOk

`func (o *UploadRequestObject) GetMimeTypeOk() (*string, bool)`

GetMimeTypeOk returns a tuple with the MimeType field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMimeType

`func (o *UploadRequestObject) SetMimeType(v string)`

SetMimeType sets MimeType field to given value.


### GetChecksums

`func (o *UploadRequestObject) GetChecksums() []Checksum`

GetChecksums returns the Checksums field if non-nil, zero value otherwise.

### GetChecksumsOk

`func (o *UploadRequestObject) GetChecksumsOk() (*[]Checksum, bool)`

GetChecksumsOk returns a tuple with the Checksums field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChecksums

`func (o *UploadRequestObject) SetChecksums(v []Checksum)`

SetChecksums sets Checksums field to given value.


### GetDescription

`func (o *UploadRequestObject) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *UploadRequestObject) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *UploadRequestObject) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *UploadRequestObject) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetAliases

`func (o *UploadRequestObject) GetAliases() []string`

GetAliases returns the Aliases field if non-nil, zero value otherwise.

### GetAliasesOk

`func (o *UploadRequestObject) GetAliasesOk() (*[]string, bool)`

GetAliasesOk returns a tuple with the Aliases field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAliases

`func (o *UploadRequestObject) SetAliases(v []string)`

SetAliases sets Aliases field to given value.

### HasAliases

`func (o *UploadRequestObject) HasAliases() bool`

HasAliases returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


