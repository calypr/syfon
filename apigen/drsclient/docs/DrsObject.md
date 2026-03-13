# DrsObject

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | An identifier unique to this &#x60;DrsObject&#x60; | 
**Name** | Pointer to **string** | A string that can be used to name a &#x60;DrsObject&#x60;. This string is made up of uppercase and lowercase letters, decimal digits, hyphen, period, and underscore [A-Za-z0-9.-_]. See http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282[portable filenames]. | [optional] 
**SelfUri** | **string** | A drs:// hostname-based URI, as defined in the DRS documentation, that tells clients how to access this object. The intent of this field is to make DRS objects self-contained, and therefore easier for clients to store and pass around.  For example, if you arrive at this DRS JSON by resolving a compact identifier-based DRS URI, the &#x60;self_uri&#x60; presents you with a hostname and properly encoded DRS ID for use in subsequent &#x60;access&#x60; endpoint calls. | 
**Size** | **int64** | For blobs, the blob size in bytes. For bundles, the cumulative size, in bytes, of items in the &#x60;contents&#x60; field. | 
**CreatedTime** | **time.Time** | Timestamp of content creation in RFC3339. (This is the creation time of the underlying content, not of the JSON object.) | 
**UpdatedTime** | Pointer to **time.Time** | Timestamp of content update in RFC3339, identical to &#x60;created_time&#x60; in systems that do not support updates. (This is the update time of the underlying content, not of the JSON object.) | [optional] 
**Version** | Pointer to **string** | A string representing a version. (Some systems may use checksum, a RFC3339 timestamp, or an incrementing version number.) | [optional] 
**MimeType** | Pointer to **string** | A string providing the mime-type of the &#x60;DrsObject&#x60;. | [optional] 
**Checksums** | [**[]Checksum**](Checksum.md) | The checksum of the &#x60;DrsObject&#x60;. At least one checksum must be provided. For blobs, the checksum is computed over the bytes in the blob. For bundles, the checksum is computed over a sorted concatenation of the checksums of its top-level contained objects (not recursive, names not included). The list of checksums is sorted alphabetically (hex-code) before concatenation and a further checksum is performed on the concatenated checksum value. For example, if a bundle contains blobs with the following checksums: md5(blob1) &#x3D; 72794b6d md5(blob2) &#x3D; 5e089d29 Then the checksum of the bundle is: md5( concat( sort( md5(blob1), md5(blob2) ) ) ) &#x3D; md5( concat( sort( 72794b6d, 5e089d29 ) ) ) &#x3D; md5( concat( 5e089d29, 72794b6d ) ) &#x3D; md5( 5e089d2972794b6d ) &#x3D; f7a29a04 | 
**AccessMethods** | Pointer to [**[]AccessMethod**](AccessMethod.md) | The list of access methods that can be used to fetch the &#x60;DrsObject&#x60;. Required for single blobs; optional for bundles. | [optional] 
**Contents** | Pointer to [**[]ContentsObject**](ContentsObject.md) | If not set, this &#x60;DrsObject&#x60; is a single blob. If set, this &#x60;DrsObject&#x60; is a bundle containing the listed &#x60;ContentsObject&#x60; s (some of which may be further nested). | [optional] 
**Description** | Pointer to **string** | A human readable description of the &#x60;DrsObject&#x60;. | [optional] 
**Aliases** | Pointer to **[]string** | A list of strings that can be used to find other metadata about this &#x60;DrsObject&#x60; from external metadata sources. These aliases can be used to represent secondary accession numbers or external GUIDs. | [optional] 

## Methods

### NewDrsObject

`func NewDrsObject(id string, selfUri string, size int64, createdTime time.Time, checksums []Checksum, ) *DrsObject`

NewDrsObject instantiates a new DrsObject object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDrsObjectWithDefaults

`func NewDrsObjectWithDefaults() *DrsObject`

NewDrsObjectWithDefaults instantiates a new DrsObject object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *DrsObject) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *DrsObject) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *DrsObject) SetId(v string)`

SetId sets Id field to given value.


### GetName

`func (o *DrsObject) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *DrsObject) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *DrsObject) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *DrsObject) HasName() bool`

HasName returns a boolean if a field has been set.

### GetSelfUri

`func (o *DrsObject) GetSelfUri() string`

GetSelfUri returns the SelfUri field if non-nil, zero value otherwise.

### GetSelfUriOk

`func (o *DrsObject) GetSelfUriOk() (*string, bool)`

GetSelfUriOk returns a tuple with the SelfUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSelfUri

`func (o *DrsObject) SetSelfUri(v string)`

SetSelfUri sets SelfUri field to given value.


### GetSize

`func (o *DrsObject) GetSize() int64`

GetSize returns the Size field if non-nil, zero value otherwise.

### GetSizeOk

`func (o *DrsObject) GetSizeOk() (*int64, bool)`

GetSizeOk returns a tuple with the Size field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSize

`func (o *DrsObject) SetSize(v int64)`

SetSize sets Size field to given value.


### GetCreatedTime

`func (o *DrsObject) GetCreatedTime() time.Time`

GetCreatedTime returns the CreatedTime field if non-nil, zero value otherwise.

### GetCreatedTimeOk

`func (o *DrsObject) GetCreatedTimeOk() (*time.Time, bool)`

GetCreatedTimeOk returns a tuple with the CreatedTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedTime

`func (o *DrsObject) SetCreatedTime(v time.Time)`

SetCreatedTime sets CreatedTime field to given value.


### GetUpdatedTime

`func (o *DrsObject) GetUpdatedTime() time.Time`

GetUpdatedTime returns the UpdatedTime field if non-nil, zero value otherwise.

### GetUpdatedTimeOk

`func (o *DrsObject) GetUpdatedTimeOk() (*time.Time, bool)`

GetUpdatedTimeOk returns a tuple with the UpdatedTime field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUpdatedTime

`func (o *DrsObject) SetUpdatedTime(v time.Time)`

SetUpdatedTime sets UpdatedTime field to given value.

### HasUpdatedTime

`func (o *DrsObject) HasUpdatedTime() bool`

HasUpdatedTime returns a boolean if a field has been set.

### GetVersion

`func (o *DrsObject) GetVersion() string`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *DrsObject) GetVersionOk() (*string, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *DrsObject) SetVersion(v string)`

SetVersion sets Version field to given value.

### HasVersion

`func (o *DrsObject) HasVersion() bool`

HasVersion returns a boolean if a field has been set.

### GetMimeType

`func (o *DrsObject) GetMimeType() string`

GetMimeType returns the MimeType field if non-nil, zero value otherwise.

### GetMimeTypeOk

`func (o *DrsObject) GetMimeTypeOk() (*string, bool)`

GetMimeTypeOk returns a tuple with the MimeType field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMimeType

`func (o *DrsObject) SetMimeType(v string)`

SetMimeType sets MimeType field to given value.

### HasMimeType

`func (o *DrsObject) HasMimeType() bool`

HasMimeType returns a boolean if a field has been set.

### GetChecksums

`func (o *DrsObject) GetChecksums() []Checksum`

GetChecksums returns the Checksums field if non-nil, zero value otherwise.

### GetChecksumsOk

`func (o *DrsObject) GetChecksumsOk() (*[]Checksum, bool)`

GetChecksumsOk returns a tuple with the Checksums field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChecksums

`func (o *DrsObject) SetChecksums(v []Checksum)`

SetChecksums sets Checksums field to given value.


### GetAccessMethods

`func (o *DrsObject) GetAccessMethods() []AccessMethod`

GetAccessMethods returns the AccessMethods field if non-nil, zero value otherwise.

### GetAccessMethodsOk

`func (o *DrsObject) GetAccessMethodsOk() (*[]AccessMethod, bool)`

GetAccessMethodsOk returns a tuple with the AccessMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessMethods

`func (o *DrsObject) SetAccessMethods(v []AccessMethod)`

SetAccessMethods sets AccessMethods field to given value.

### HasAccessMethods

`func (o *DrsObject) HasAccessMethods() bool`

HasAccessMethods returns a boolean if a field has been set.

### GetContents

`func (o *DrsObject) GetContents() []ContentsObject`

GetContents returns the Contents field if non-nil, zero value otherwise.

### GetContentsOk

`func (o *DrsObject) GetContentsOk() (*[]ContentsObject, bool)`

GetContentsOk returns a tuple with the Contents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContents

`func (o *DrsObject) SetContents(v []ContentsObject)`

SetContents sets Contents field to given value.

### HasContents

`func (o *DrsObject) HasContents() bool`

HasContents returns a boolean if a field has been set.

### GetDescription

`func (o *DrsObject) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *DrsObject) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *DrsObject) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *DrsObject) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetAliases

`func (o *DrsObject) GetAliases() []string`

GetAliases returns the Aliases field if non-nil, zero value otherwise.

### GetAliasesOk

`func (o *DrsObject) GetAliasesOk() (*[]string, bool)`

GetAliasesOk returns a tuple with the Aliases field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAliases

`func (o *DrsObject) SetAliases(v []string)`

SetAliases sets Aliases field to given value.

### HasAliases

`func (o *DrsObject) HasAliases() bool`

HasAliases returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


