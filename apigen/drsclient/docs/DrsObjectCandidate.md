# DrsObjectCandidate

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | Pointer to **string** | A string that can be used to name a &#x60;DrsObject&#x60;. This string is made up of uppercase and lowercase letters, decimal digits, hyphen, period, and underscore [A-Za-z0-9.-_]. See http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282[portable filenames]. | [optional] 
**Size** | **int64** | For blobs, the blob size in bytes. For bundles, the cumulative size, in bytes, of items in the &#x60;contents&#x60; field. | 
**Version** | Pointer to **string** | A string representing a version. (Some systems may use checksum, a RFC3339 timestamp, or an incrementing version number.) | [optional] 
**MimeType** | Pointer to **string** | A string providing the mime-type of the &#x60;DrsObject&#x60;. | [optional] 
**Checksums** | [**[]Checksum**](Checksum.md) | The checksum of the &#x60;DrsObject&#x60;. At least one checksum must be provided. For blobs, the checksum is computed over the bytes in the blob. For bundles, the checksum is computed over a sorted concatenation of the checksums of its top-level contained objects (not recursive, names not included). The list of checksums is sorted alphabetically (hex-code) before concatenation and a further checksum is performed on the concatenated checksum value. For example, if a bundle contains blobs with the following checksums: md5(blob1) &#x3D; 72794b6d md5(blob2) &#x3D; 5e089d29 Then the checksum of the bundle is: md5( concat( sort( md5(blob1), md5(blob2) ) ) ) &#x3D; md5( concat( sort( 72794b6d, 5e089d29 ) ) ) &#x3D; md5( concat( 5e089d29, 72794b6d ) ) &#x3D; md5( 5e089d2972794b6d ) &#x3D; f7a29a04 | 
**AccessMethods** | Pointer to [**[]AccessMethod**](AccessMethod.md) | The list of access methods that can be used to fetch the &#x60;DrsObject&#x60;. Required for single blobs; optional for bundles. | [optional] 
**Contents** | Pointer to [**[]ContentsObject**](ContentsObject.md) | If not set, this &#x60;DrsObject&#x60; is a single blob. If set, this &#x60;DrsObject&#x60; is a bundle containing the listed &#x60;ContentsObject&#x60; s (some of which may be further nested). | [optional] 
**Description** | Pointer to **string** | A human readable description of the &#x60;DrsObject&#x60;. | [optional] 
**Aliases** | Pointer to **[]string** | A list of strings that can be used to find other metadata about this &#x60;DrsObject&#x60; from external metadata sources. These aliases can be used to represent secondary accession numbers or external GUIDs. | [optional] 

## Methods

### NewDrsObjectCandidate

`func NewDrsObjectCandidate(size int64, checksums []Checksum, ) *DrsObjectCandidate`

NewDrsObjectCandidate instantiates a new DrsObjectCandidate object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDrsObjectCandidateWithDefaults

`func NewDrsObjectCandidateWithDefaults() *DrsObjectCandidate`

NewDrsObjectCandidateWithDefaults instantiates a new DrsObjectCandidate object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *DrsObjectCandidate) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *DrsObjectCandidate) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *DrsObjectCandidate) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *DrsObjectCandidate) HasName() bool`

HasName returns a boolean if a field has been set.

### GetSize

`func (o *DrsObjectCandidate) GetSize() int64`

GetSize returns the Size field if non-nil, zero value otherwise.

### GetSizeOk

`func (o *DrsObjectCandidate) GetSizeOk() (*int64, bool)`

GetSizeOk returns a tuple with the Size field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSize

`func (o *DrsObjectCandidate) SetSize(v int64)`

SetSize sets Size field to given value.


### GetVersion

`func (o *DrsObjectCandidate) GetVersion() string`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *DrsObjectCandidate) GetVersionOk() (*string, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *DrsObjectCandidate) SetVersion(v string)`

SetVersion sets Version field to given value.

### HasVersion

`func (o *DrsObjectCandidate) HasVersion() bool`

HasVersion returns a boolean if a field has been set.

### GetMimeType

`func (o *DrsObjectCandidate) GetMimeType() string`

GetMimeType returns the MimeType field if non-nil, zero value otherwise.

### GetMimeTypeOk

`func (o *DrsObjectCandidate) GetMimeTypeOk() (*string, bool)`

GetMimeTypeOk returns a tuple with the MimeType field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMimeType

`func (o *DrsObjectCandidate) SetMimeType(v string)`

SetMimeType sets MimeType field to given value.

### HasMimeType

`func (o *DrsObjectCandidate) HasMimeType() bool`

HasMimeType returns a boolean if a field has been set.

### GetChecksums

`func (o *DrsObjectCandidate) GetChecksums() []Checksum`

GetChecksums returns the Checksums field if non-nil, zero value otherwise.

### GetChecksumsOk

`func (o *DrsObjectCandidate) GetChecksumsOk() (*[]Checksum, bool)`

GetChecksumsOk returns a tuple with the Checksums field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChecksums

`func (o *DrsObjectCandidate) SetChecksums(v []Checksum)`

SetChecksums sets Checksums field to given value.


### GetAccessMethods

`func (o *DrsObjectCandidate) GetAccessMethods() []AccessMethod`

GetAccessMethods returns the AccessMethods field if non-nil, zero value otherwise.

### GetAccessMethodsOk

`func (o *DrsObjectCandidate) GetAccessMethodsOk() (*[]AccessMethod, bool)`

GetAccessMethodsOk returns a tuple with the AccessMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessMethods

`func (o *DrsObjectCandidate) SetAccessMethods(v []AccessMethod)`

SetAccessMethods sets AccessMethods field to given value.

### HasAccessMethods

`func (o *DrsObjectCandidate) HasAccessMethods() bool`

HasAccessMethods returns a boolean if a field has been set.

### GetContents

`func (o *DrsObjectCandidate) GetContents() []ContentsObject`

GetContents returns the Contents field if non-nil, zero value otherwise.

### GetContentsOk

`func (o *DrsObjectCandidate) GetContentsOk() (*[]ContentsObject, bool)`

GetContentsOk returns a tuple with the Contents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContents

`func (o *DrsObjectCandidate) SetContents(v []ContentsObject)`

SetContents sets Contents field to given value.

### HasContents

`func (o *DrsObjectCandidate) HasContents() bool`

HasContents returns a boolean if a field has been set.

### GetDescription

`func (o *DrsObjectCandidate) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *DrsObjectCandidate) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *DrsObjectCandidate) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *DrsObjectCandidate) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetAliases

`func (o *DrsObjectCandidate) GetAliases() []string`

GetAliases returns the Aliases field if non-nil, zero value otherwise.

### GetAliasesOk

`func (o *DrsObjectCandidate) GetAliasesOk() (*[]string, bool)`

GetAliasesOk returns a tuple with the Aliases field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAliases

`func (o *DrsObjectCandidate) SetAliases(v []string)`

SetAliases sets Aliases field to given value.

### HasAliases

`func (o *DrsObjectCandidate) HasAliases() bool`

HasAliases returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


