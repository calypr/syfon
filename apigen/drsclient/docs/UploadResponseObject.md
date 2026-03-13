# UploadResponseObject

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | The name of the file | 
**Size** | **int64** | Size of the file in bytes | 
**MimeType** | **string** | MIME type of the file | 
**Checksums** | [**[]Checksum**](Checksum.md) | Array of checksums for file integrity verification | 
**Description** | Pointer to **string** | Optional description of the file | [optional] 
**Aliases** | Pointer to **[]string** | Optional array of alternative names | [optional] 
**UploadMethods** | Pointer to [**[]UploadMethod**](UploadMethod.md) | Available methods for uploading this file | [optional] 

## Methods

### NewUploadResponseObject

`func NewUploadResponseObject(name string, size int64, mimeType string, checksums []Checksum, ) *UploadResponseObject`

NewUploadResponseObject instantiates a new UploadResponseObject object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUploadResponseObjectWithDefaults

`func NewUploadResponseObjectWithDefaults() *UploadResponseObject`

NewUploadResponseObjectWithDefaults instantiates a new UploadResponseObject object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *UploadResponseObject) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *UploadResponseObject) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *UploadResponseObject) SetName(v string)`

SetName sets Name field to given value.


### GetSize

`func (o *UploadResponseObject) GetSize() int64`

GetSize returns the Size field if non-nil, zero value otherwise.

### GetSizeOk

`func (o *UploadResponseObject) GetSizeOk() (*int64, bool)`

GetSizeOk returns a tuple with the Size field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSize

`func (o *UploadResponseObject) SetSize(v int64)`

SetSize sets Size field to given value.


### GetMimeType

`func (o *UploadResponseObject) GetMimeType() string`

GetMimeType returns the MimeType field if non-nil, zero value otherwise.

### GetMimeTypeOk

`func (o *UploadResponseObject) GetMimeTypeOk() (*string, bool)`

GetMimeTypeOk returns a tuple with the MimeType field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMimeType

`func (o *UploadResponseObject) SetMimeType(v string)`

SetMimeType sets MimeType field to given value.


### GetChecksums

`func (o *UploadResponseObject) GetChecksums() []Checksum`

GetChecksums returns the Checksums field if non-nil, zero value otherwise.

### GetChecksumsOk

`func (o *UploadResponseObject) GetChecksumsOk() (*[]Checksum, bool)`

GetChecksumsOk returns a tuple with the Checksums field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChecksums

`func (o *UploadResponseObject) SetChecksums(v []Checksum)`

SetChecksums sets Checksums field to given value.


### GetDescription

`func (o *UploadResponseObject) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *UploadResponseObject) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *UploadResponseObject) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *UploadResponseObject) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetAliases

`func (o *UploadResponseObject) GetAliases() []string`

GetAliases returns the Aliases field if non-nil, zero value otherwise.

### GetAliasesOk

`func (o *UploadResponseObject) GetAliasesOk() (*[]string, bool)`

GetAliasesOk returns a tuple with the Aliases field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAliases

`func (o *UploadResponseObject) SetAliases(v []string)`

SetAliases sets Aliases field to given value.

### HasAliases

`func (o *UploadResponseObject) HasAliases() bool`

HasAliases returns a boolean if a field has been set.

### GetUploadMethods

`func (o *UploadResponseObject) GetUploadMethods() []UploadMethod`

GetUploadMethods returns the UploadMethods field if non-nil, zero value otherwise.

### GetUploadMethodsOk

`func (o *UploadResponseObject) GetUploadMethodsOk() (*[]UploadMethod, bool)`

GetUploadMethodsOk returns a tuple with the UploadMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUploadMethods

`func (o *UploadResponseObject) SetUploadMethods(v []UploadMethod)`

SetUploadMethods sets UploadMethods field to given value.

### HasUploadMethods

`func (o *UploadResponseObject) HasUploadMethods() bool`

HasUploadMethods returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


