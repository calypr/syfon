# AccessMethodUpdateRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**AccessMethods** | [**[]AccessMethod**](AccessMethod.md) | New access methods for the DRS object | 
**Passports** | Pointer to **[]string** | Optional GA4GH Passport JWTs for authorization | [optional] 

## Methods

### NewAccessMethodUpdateRequest

`func NewAccessMethodUpdateRequest(accessMethods []AccessMethod, ) *AccessMethodUpdateRequest`

NewAccessMethodUpdateRequest instantiates a new AccessMethodUpdateRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccessMethodUpdateRequestWithDefaults

`func NewAccessMethodUpdateRequestWithDefaults() *AccessMethodUpdateRequest`

NewAccessMethodUpdateRequestWithDefaults instantiates a new AccessMethodUpdateRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAccessMethods

`func (o *AccessMethodUpdateRequest) GetAccessMethods() []AccessMethod`

GetAccessMethods returns the AccessMethods field if non-nil, zero value otherwise.

### GetAccessMethodsOk

`func (o *AccessMethodUpdateRequest) GetAccessMethodsOk() (*[]AccessMethod, bool)`

GetAccessMethodsOk returns a tuple with the AccessMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessMethods

`func (o *AccessMethodUpdateRequest) SetAccessMethods(v []AccessMethod)`

SetAccessMethods sets AccessMethods field to given value.


### GetPassports

`func (o *AccessMethodUpdateRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *AccessMethodUpdateRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *AccessMethodUpdateRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *AccessMethodUpdateRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


