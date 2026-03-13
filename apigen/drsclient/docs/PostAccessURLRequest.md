# PostAccessURLRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Passports** | Pointer to **[]string** | the encoded JWT GA4GH Passport that contains embedded Visas.  The overall JWT is signed as are the individual Passport Visas. | [optional] 

## Methods

### NewPostAccessURLRequest

`func NewPostAccessURLRequest() *PostAccessURLRequest`

NewPostAccessURLRequest instantiates a new PostAccessURLRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPostAccessURLRequestWithDefaults

`func NewPostAccessURLRequestWithDefaults() *PostAccessURLRequest`

NewPostAccessURLRequestWithDefaults instantiates a new PostAccessURLRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPassports

`func (o *PostAccessURLRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *PostAccessURLRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *PostAccessURLRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *PostAccessURLRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


