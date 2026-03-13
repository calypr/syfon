# DrsService

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**MaxBulkRequestLength** | **int32** | DEPRECATED - In 2.0 this will move to under the drs section of service info and not at the root level. The max length the bulk request endpoints can handle (&gt;&#x3D; 1) before generating a 413 error e.g. how long can the arrays bulk_object_ids and bulk_object_access_ids be for this server. | 
**Type** | [**DrsServiceType**](DrsServiceType.md) |  | 
**Drs** | Pointer to [**DrsServiceDrs**](DrsServiceDrs.md) |  | [optional] 

## Methods

### NewDrsService

`func NewDrsService(maxBulkRequestLength int32, type_ DrsServiceType, ) *DrsService`

NewDrsService instantiates a new DrsService object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewDrsServiceWithDefaults

`func NewDrsServiceWithDefaults() *DrsService`

NewDrsServiceWithDefaults instantiates a new DrsService object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetMaxBulkRequestLength

`func (o *DrsService) GetMaxBulkRequestLength() int32`

GetMaxBulkRequestLength returns the MaxBulkRequestLength field if non-nil, zero value otherwise.

### GetMaxBulkRequestLengthOk

`func (o *DrsService) GetMaxBulkRequestLengthOk() (*int32, bool)`

GetMaxBulkRequestLengthOk returns a tuple with the MaxBulkRequestLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxBulkRequestLength

`func (o *DrsService) SetMaxBulkRequestLength(v int32)`

SetMaxBulkRequestLength sets MaxBulkRequestLength field to given value.


### GetType

`func (o *DrsService) GetType() DrsServiceType`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *DrsService) GetTypeOk() (*DrsServiceType, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *DrsService) SetType(v DrsServiceType)`

SetType sets Type field to given value.


### GetDrs

`func (o *DrsService) GetDrs() DrsServiceDrs`

GetDrs returns the Drs field if non-nil, zero value otherwise.

### GetDrsOk

`func (o *DrsService) GetDrsOk() (*DrsServiceDrs, bool)`

GetDrsOk returns a tuple with the Drs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrs

`func (o *DrsService) SetDrs(v DrsServiceDrs)`

SetDrs sets Drs field to given value.

### HasDrs

`func (o *DrsService) HasDrs() bool`

HasDrs returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


