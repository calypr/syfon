# Summary

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Requested** | Pointer to **int32** | Number of items requested. | [optional] 
**Resolved** | Pointer to **int32** | Number of objects resolved. | [optional] 
**Unresolved** | Pointer to **int32** | Number of objects not resolved. | [optional] 

## Methods

### NewSummary

`func NewSummary() *Summary`

NewSummary instantiates a new Summary object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSummaryWithDefaults

`func NewSummaryWithDefaults() *Summary`

NewSummaryWithDefaults instantiates a new Summary object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetRequested

`func (o *Summary) GetRequested() int32`

GetRequested returns the Requested field if non-nil, zero value otherwise.

### GetRequestedOk

`func (o *Summary) GetRequestedOk() (*int32, bool)`

GetRequestedOk returns a tuple with the Requested field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequested

`func (o *Summary) SetRequested(v int32)`

SetRequested sets Requested field to given value.

### HasRequested

`func (o *Summary) HasRequested() bool`

HasRequested returns a boolean if a field has been set.

### GetResolved

`func (o *Summary) GetResolved() int32`

GetResolved returns the Resolved field if non-nil, zero value otherwise.

### GetResolvedOk

`func (o *Summary) GetResolvedOk() (*int32, bool)`

GetResolvedOk returns a tuple with the Resolved field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResolved

`func (o *Summary) SetResolved(v int32)`

SetResolved sets Resolved field to given value.

### HasResolved

`func (o *Summary) HasResolved() bool`

HasResolved returns a boolean if a field has been set.

### GetUnresolved

`func (o *Summary) GetUnresolved() int32`

GetUnresolved returns the Unresolved field if non-nil, zero value otherwise.

### GetUnresolvedOk

`func (o *Summary) GetUnresolvedOk() (*int32, bool)`

GetUnresolvedOk returns a tuple with the Unresolved field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUnresolved

`func (o *Summary) SetUnresolved(v int32)`

SetUnresolved sets Unresolved field to given value.

### HasUnresolved

`func (o *Summary) HasUnresolved() bool`

HasUnresolved returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


