# GetBulkObjectsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Passports** | Pointer to **[]string** | the encoded JWT GA4GH Passport that contains embedded Visas.  The overall JWT is signed as are the individual Passport Visas. | [optional] 
**BulkObjectIds** | **[]string** | An array of ObjectIDs to retrieve metadata for | 

## Methods

### NewGetBulkObjectsRequest

`func NewGetBulkObjectsRequest(bulkObjectIds []string, ) *GetBulkObjectsRequest`

NewGetBulkObjectsRequest instantiates a new GetBulkObjectsRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetBulkObjectsRequestWithDefaults

`func NewGetBulkObjectsRequestWithDefaults() *GetBulkObjectsRequest`

NewGetBulkObjectsRequestWithDefaults instantiates a new GetBulkObjectsRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPassports

`func (o *GetBulkObjectsRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *GetBulkObjectsRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *GetBulkObjectsRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *GetBulkObjectsRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.

### GetBulkObjectIds

`func (o *GetBulkObjectsRequest) GetBulkObjectIds() []string`

GetBulkObjectIds returns the BulkObjectIds field if non-nil, zero value otherwise.

### GetBulkObjectIdsOk

`func (o *GetBulkObjectsRequest) GetBulkObjectIdsOk() (*[]string, bool)`

GetBulkObjectIdsOk returns a tuple with the BulkObjectIds field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBulkObjectIds

`func (o *GetBulkObjectsRequest) SetBulkObjectIds(v []string)`

SetBulkObjectIds sets BulkObjectIds field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


