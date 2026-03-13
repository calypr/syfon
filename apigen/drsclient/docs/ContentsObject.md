# ContentsObject

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | A name declared by the bundle author that must be used when materialising this object, overriding any name directly associated with the object itself. The name must be unique within the containing bundle. This string is made up of uppercase and lowercase letters, decimal digits, hyphen, period, and underscore [A-Za-z0-9.-_]. See http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap03.html#tag_03_282[portable filenames]. | 
**Id** | Pointer to **string** | A DRS identifier of a &#x60;DrsObject&#x60; (either a single blob or a nested bundle). If this ContentsObject is an object within a nested bundle, then the id is optional. Otherwise, the id is required. | [optional] 
**DrsUri** | Pointer to **[]string** | A list of full DRS identifier URI paths that may be used to obtain the object. These URIs may be external to this DRS instance. | [optional] 
**Contents** | Pointer to [**[]ContentsObject**](ContentsObject.md) | If this ContentsObject describes a nested bundle and the caller specified \&quot;?expand&#x3D;true\&quot; on the request, then this contents array must be present and describe the objects within the nested bundle. | [optional] 

## Methods

### NewContentsObject

`func NewContentsObject(name string, ) *ContentsObject`

NewContentsObject instantiates a new ContentsObject object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewContentsObjectWithDefaults

`func NewContentsObjectWithDefaults() *ContentsObject`

NewContentsObjectWithDefaults instantiates a new ContentsObject object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *ContentsObject) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ContentsObject) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ContentsObject) SetName(v string)`

SetName sets Name field to given value.


### GetId

`func (o *ContentsObject) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ContentsObject) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ContentsObject) SetId(v string)`

SetId sets Id field to given value.

### HasId

`func (o *ContentsObject) HasId() bool`

HasId returns a boolean if a field has been set.

### GetDrsUri

`func (o *ContentsObject) GetDrsUri() []string`

GetDrsUri returns the DrsUri field if non-nil, zero value otherwise.

### GetDrsUriOk

`func (o *ContentsObject) GetDrsUriOk() (*[]string, bool)`

GetDrsUriOk returns a tuple with the DrsUri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrsUri

`func (o *ContentsObject) SetDrsUri(v []string)`

SetDrsUri sets DrsUri field to given value.

### HasDrsUri

`func (o *ContentsObject) HasDrsUri() bool`

HasDrsUri returns a boolean if a field has been set.

### GetContents

`func (o *ContentsObject) GetContents() []ContentsObject`

GetContents returns the Contents field if non-nil, zero value otherwise.

### GetContentsOk

`func (o *ContentsObject) GetContentsOk() (*[]ContentsObject, bool)`

GetContentsOk returns a tuple with the Contents field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContents

`func (o *ContentsObject) SetContents(v []ContentsObject)`

SetContents sets Contents field to given value.

### HasContents

`func (o *ContentsObject) HasContents() bool`

HasContents returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


