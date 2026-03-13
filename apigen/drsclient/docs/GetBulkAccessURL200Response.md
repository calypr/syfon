# GetBulkAccessURL200Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Summary** | Pointer to [**Summary**](Summary.md) |  | [optional] 
**UnresolvedDrsObjects** | Pointer to [**[]UnresolvedInner**](UnresolvedInner.md) | Error codes for each unresolved drs objects. | [optional] 
**ResolvedDrsObjectAccessUrls** | Pointer to [**[]BulkAccessURL**](BulkAccessURL.md) |  | [optional] 

## Methods

### NewGetBulkAccessURL200Response

`func NewGetBulkAccessURL200Response() *GetBulkAccessURL200Response`

NewGetBulkAccessURL200Response instantiates a new GetBulkAccessURL200Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetBulkAccessURL200ResponseWithDefaults

`func NewGetBulkAccessURL200ResponseWithDefaults() *GetBulkAccessURL200Response`

NewGetBulkAccessURL200ResponseWithDefaults instantiates a new GetBulkAccessURL200Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetSummary

`func (o *GetBulkAccessURL200Response) GetSummary() Summary`

GetSummary returns the Summary field if non-nil, zero value otherwise.

### GetSummaryOk

`func (o *GetBulkAccessURL200Response) GetSummaryOk() (*Summary, bool)`

GetSummaryOk returns a tuple with the Summary field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSummary

`func (o *GetBulkAccessURL200Response) SetSummary(v Summary)`

SetSummary sets Summary field to given value.

### HasSummary

`func (o *GetBulkAccessURL200Response) HasSummary() bool`

HasSummary returns a boolean if a field has been set.

### GetUnresolvedDrsObjects

`func (o *GetBulkAccessURL200Response) GetUnresolvedDrsObjects() []UnresolvedInner`

GetUnresolvedDrsObjects returns the UnresolvedDrsObjects field if non-nil, zero value otherwise.

### GetUnresolvedDrsObjectsOk

`func (o *GetBulkAccessURL200Response) GetUnresolvedDrsObjectsOk() (*[]UnresolvedInner, bool)`

GetUnresolvedDrsObjectsOk returns a tuple with the UnresolvedDrsObjects field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUnresolvedDrsObjects

`func (o *GetBulkAccessURL200Response) SetUnresolvedDrsObjects(v []UnresolvedInner)`

SetUnresolvedDrsObjects sets UnresolvedDrsObjects field to given value.

### HasUnresolvedDrsObjects

`func (o *GetBulkAccessURL200Response) HasUnresolvedDrsObjects() bool`

HasUnresolvedDrsObjects returns a boolean if a field has been set.

### GetResolvedDrsObjectAccessUrls

`func (o *GetBulkAccessURL200Response) GetResolvedDrsObjectAccessUrls() []BulkAccessURL`

GetResolvedDrsObjectAccessUrls returns the ResolvedDrsObjectAccessUrls field if non-nil, zero value otherwise.

### GetResolvedDrsObjectAccessUrlsOk

`func (o *GetBulkAccessURL200Response) GetResolvedDrsObjectAccessUrlsOk() (*[]BulkAccessURL, bool)`

GetResolvedDrsObjectAccessUrlsOk returns a tuple with the ResolvedDrsObjectAccessUrls field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResolvedDrsObjectAccessUrls

`func (o *GetBulkAccessURL200Response) SetResolvedDrsObjectAccessUrls(v []BulkAccessURL)`

SetResolvedDrsObjectAccessUrls sets ResolvedDrsObjectAccessUrls field to given value.

### HasResolvedDrsObjectAccessUrls

`func (o *GetBulkAccessURL200Response) HasResolvedDrsObjectAccessUrls() bool`

HasResolvedDrsObjectAccessUrls returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


