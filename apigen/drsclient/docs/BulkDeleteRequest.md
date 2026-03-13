# BulkDeleteRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**BulkObjectIds** | **[]string** | Array of DRS object IDs to delete | 
**Passports** | Pointer to **[]string** | the encoded JWT GA4GH Passport that contains embedded Visas.  The overall JWT is signed as are the individual Passport Visas. | [optional] 
**DeleteStorageData** | Pointer to **bool** | If true, delete both DRS object metadata and underlying storage data (follows server&#39;s deleteStorageDataSupported capability). If false (default), only delete DRS object metadata while preserving underlying storage data. Clients must explicitly set this to true to enable storage data deletion, ensuring intentional choice for this potentially destructive operation. | [optional] [default to false]

## Methods

### NewBulkDeleteRequest

`func NewBulkDeleteRequest(bulkObjectIds []string, ) *BulkDeleteRequest`

NewBulkDeleteRequest instantiates a new BulkDeleteRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBulkDeleteRequestWithDefaults

`func NewBulkDeleteRequestWithDefaults() *BulkDeleteRequest`

NewBulkDeleteRequestWithDefaults instantiates a new BulkDeleteRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetBulkObjectIds

`func (o *BulkDeleteRequest) GetBulkObjectIds() []string`

GetBulkObjectIds returns the BulkObjectIds field if non-nil, zero value otherwise.

### GetBulkObjectIdsOk

`func (o *BulkDeleteRequest) GetBulkObjectIdsOk() (*[]string, bool)`

GetBulkObjectIdsOk returns a tuple with the BulkObjectIds field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBulkObjectIds

`func (o *BulkDeleteRequest) SetBulkObjectIds(v []string)`

SetBulkObjectIds sets BulkObjectIds field to given value.


### GetPassports

`func (o *BulkDeleteRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *BulkDeleteRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *BulkDeleteRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *BulkDeleteRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.

### GetDeleteStorageData

`func (o *BulkDeleteRequest) GetDeleteStorageData() bool`

GetDeleteStorageData returns the DeleteStorageData field if non-nil, zero value otherwise.

### GetDeleteStorageDataOk

`func (o *BulkDeleteRequest) GetDeleteStorageDataOk() (*bool, bool)`

GetDeleteStorageDataOk returns a tuple with the DeleteStorageData field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDeleteStorageData

`func (o *BulkDeleteRequest) SetDeleteStorageData(v bool)`

SetDeleteStorageData sets DeleteStorageData field to given value.

### HasDeleteStorageData

`func (o *BulkDeleteRequest) HasDeleteStorageData() bool`

HasDeleteStorageData returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


