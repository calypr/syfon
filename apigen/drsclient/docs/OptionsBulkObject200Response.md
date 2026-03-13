# OptionsBulkObject200Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Summary** | Pointer to [**Summary**](Summary.md) |  | [optional] 
**UnresolvedDrsObjects** | Pointer to [**[]UnresolvedInner**](UnresolvedInner.md) | Error codes for each unresolved drs objects. | [optional] 
**ResolvedDrsObject** | Pointer to [**[]Authorizations**](Authorizations.md) |  | [optional] 

## Methods

### NewOptionsBulkObject200Response

`func NewOptionsBulkObject200Response() *OptionsBulkObject200Response`

NewOptionsBulkObject200Response instantiates a new OptionsBulkObject200Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewOptionsBulkObject200ResponseWithDefaults

`func NewOptionsBulkObject200ResponseWithDefaults() *OptionsBulkObject200Response`

NewOptionsBulkObject200ResponseWithDefaults instantiates a new OptionsBulkObject200Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetSummary

`func (o *OptionsBulkObject200Response) GetSummary() Summary`

GetSummary returns the Summary field if non-nil, zero value otherwise.

### GetSummaryOk

`func (o *OptionsBulkObject200Response) GetSummaryOk() (*Summary, bool)`

GetSummaryOk returns a tuple with the Summary field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSummary

`func (o *OptionsBulkObject200Response) SetSummary(v Summary)`

SetSummary sets Summary field to given value.

### HasSummary

`func (o *OptionsBulkObject200Response) HasSummary() bool`

HasSummary returns a boolean if a field has been set.

### GetUnresolvedDrsObjects

`func (o *OptionsBulkObject200Response) GetUnresolvedDrsObjects() []UnresolvedInner`

GetUnresolvedDrsObjects returns the UnresolvedDrsObjects field if non-nil, zero value otherwise.

### GetUnresolvedDrsObjectsOk

`func (o *OptionsBulkObject200Response) GetUnresolvedDrsObjectsOk() (*[]UnresolvedInner, bool)`

GetUnresolvedDrsObjectsOk returns a tuple with the UnresolvedDrsObjects field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUnresolvedDrsObjects

`func (o *OptionsBulkObject200Response) SetUnresolvedDrsObjects(v []UnresolvedInner)`

SetUnresolvedDrsObjects sets UnresolvedDrsObjects field to given value.

### HasUnresolvedDrsObjects

`func (o *OptionsBulkObject200Response) HasUnresolvedDrsObjects() bool`

HasUnresolvedDrsObjects returns a boolean if a field has been set.

### GetResolvedDrsObject

`func (o *OptionsBulkObject200Response) GetResolvedDrsObject() []Authorizations`

GetResolvedDrsObject returns the ResolvedDrsObject field if non-nil, zero value otherwise.

### GetResolvedDrsObjectOk

`func (o *OptionsBulkObject200Response) GetResolvedDrsObjectOk() (*[]Authorizations, bool)`

GetResolvedDrsObjectOk returns a tuple with the ResolvedDrsObject field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResolvedDrsObject

`func (o *OptionsBulkObject200Response) SetResolvedDrsObject(v []Authorizations)`

SetResolvedDrsObject sets ResolvedDrsObject field to given value.

### HasResolvedDrsObject

`func (o *OptionsBulkObject200Response) HasResolvedDrsObject() bool`

HasResolvedDrsObject returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


