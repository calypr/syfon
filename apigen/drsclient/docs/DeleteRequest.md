# DeleteRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Passports** | Pointer to **[]string** | the encoded JWT GA4GH Passport that contains embedded Visas.  The overall JWT is signed as are the individual Passport Visas. | [optional] 
**DeleteStorageData** | Pointer to **bool** | If true, delete both DRS object metadata and underlying storage data (follows server&#39;s deleteStorageDataSupported capability). If false (default), only delete DRS object metadata while preserving underlying storage data. Clients must explicitly set this to true to enable storage data deletion, ensuring intentional choice for this potentially destructive operation. | [optional] [default to false]

## Methods

### NewDeleteRequest

`func NewDeleteRequest() *DeleteRequest`

NewDeleteRequest instantiates a new DeleteRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDeleteRequestWithDefaults

`func NewDeleteRequestWithDefaults() *DeleteRequest`

NewDeleteRequestWithDefaults instantiates a new DeleteRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPassports

`func (o *DeleteRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *DeleteRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *DeleteRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *DeleteRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.

### GetDeleteStorageData

`func (o *DeleteRequest) GetDeleteStorageData() bool`

GetDeleteStorageData returns the DeleteStorageData field if non-nil, zero value otherwise.

### GetDeleteStorageDataOk

`func (o *DeleteRequest) GetDeleteStorageDataOk() (*bool, bool)`

GetDeleteStorageDataOk returns a tuple with the DeleteStorageData field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDeleteStorageData

`func (o *DeleteRequest) SetDeleteStorageData(v bool)`

SetDeleteStorageData sets DeleteStorageData field to given value.

### HasDeleteStorageData

`func (o *DeleteRequest) HasDeleteStorageData() bool`

HasDeleteStorageData returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


