# BulkAccessMethodUpdateRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Updates** | [**[]BulkAccessMethodUpdateRequestUpdatesInner**](BulkAccessMethodUpdateRequestUpdatesInner.md) | Array of access method updates to perform | 
**Passports** | Pointer to **[]string** | Optional GA4GH Passport JWTs for authorization | [optional] 

## Methods

### NewBulkAccessMethodUpdateRequest

`func NewBulkAccessMethodUpdateRequest(updates []BulkAccessMethodUpdateRequestUpdatesInner, ) *BulkAccessMethodUpdateRequest`

NewBulkAccessMethodUpdateRequest instantiates a new BulkAccessMethodUpdateRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBulkAccessMethodUpdateRequestWithDefaults

`func NewBulkAccessMethodUpdateRequestWithDefaults() *BulkAccessMethodUpdateRequest`

NewBulkAccessMethodUpdateRequestWithDefaults instantiates a new BulkAccessMethodUpdateRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetUpdates

`func (o *BulkAccessMethodUpdateRequest) GetUpdates() []BulkAccessMethodUpdateRequestUpdatesInner`

GetUpdates returns the Updates field if non-nil, zero value otherwise.

### GetUpdatesOk

`func (o *BulkAccessMethodUpdateRequest) GetUpdatesOk() (*[]BulkAccessMethodUpdateRequestUpdatesInner, bool)`

GetUpdatesOk returns a tuple with the Updates field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUpdates

`func (o *BulkAccessMethodUpdateRequest) SetUpdates(v []BulkAccessMethodUpdateRequestUpdatesInner)`

SetUpdates sets Updates field to given value.


### GetPassports

`func (o *BulkAccessMethodUpdateRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *BulkAccessMethodUpdateRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *BulkAccessMethodUpdateRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *BulkAccessMethodUpdateRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


