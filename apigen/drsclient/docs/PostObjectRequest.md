# PostObjectRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Expand** | Pointer to **bool** | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains other bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. | [optional] 
**Passports** | Pointer to **[]string** | the encoded JWT GA4GH Passport that contains embedded Visas.  The overall JWT is signed as are the individual Passport Visas. | [optional] 

## Methods

### NewPostObjectRequest

`func NewPostObjectRequest() *PostObjectRequest`

NewPostObjectRequest instantiates a new PostObjectRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPostObjectRequestWithDefaults

`func NewPostObjectRequestWithDefaults() *PostObjectRequest`

NewPostObjectRequestWithDefaults instantiates a new PostObjectRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetExpand

`func (o *PostObjectRequest) GetExpand() bool`

GetExpand returns the Expand field if non-nil, zero value otherwise.

### GetExpandOk

`func (o *PostObjectRequest) GetExpandOk() (*bool, bool)`

GetExpandOk returns a tuple with the Expand field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetExpand

`func (o *PostObjectRequest) SetExpand(v bool)`

SetExpand sets Expand field to given value.

### HasExpand

`func (o *PostObjectRequest) HasExpand() bool`

HasExpand returns a boolean if a field has been set.

### GetPassports

`func (o *PostObjectRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *PostObjectRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *PostObjectRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *PostObjectRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


