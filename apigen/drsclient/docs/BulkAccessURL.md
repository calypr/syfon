# BulkAccessURL

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DrsObjectId** | Pointer to **string** |  | [optional] 
**DrsAccessId** | Pointer to **string** |  | [optional] 
**Url** | **string** | A fully resolvable URL that can be used to fetch the actual object bytes. | 
**Headers** | Pointer to **[]string** | An optional list of headers to include in the HTTP request to &#x60;url&#x60;. These headers can be used to provide auth tokens required to fetch the object bytes. | [optional] 

## Methods

### NewBulkAccessURL

`func NewBulkAccessURL(url string, ) *BulkAccessURL`

NewBulkAccessURL instantiates a new BulkAccessURL object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBulkAccessURLWithDefaults

`func NewBulkAccessURLWithDefaults() *BulkAccessURL`

NewBulkAccessURLWithDefaults instantiates a new BulkAccessURL object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDrsObjectId

`func (o *BulkAccessURL) GetDrsObjectId() string`

GetDrsObjectId returns the DrsObjectId field if non-nil, zero value otherwise.

### GetDrsObjectIdOk

`func (o *BulkAccessURL) GetDrsObjectIdOk() (*string, bool)`

GetDrsObjectIdOk returns a tuple with the DrsObjectId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrsObjectId

`func (o *BulkAccessURL) SetDrsObjectId(v string)`

SetDrsObjectId sets DrsObjectId field to given value.

### HasDrsObjectId

`func (o *BulkAccessURL) HasDrsObjectId() bool`

HasDrsObjectId returns a boolean if a field has been set.

### GetDrsAccessId

`func (o *BulkAccessURL) GetDrsAccessId() string`

GetDrsAccessId returns the DrsAccessId field if non-nil, zero value otherwise.

### GetDrsAccessIdOk

`func (o *BulkAccessURL) GetDrsAccessIdOk() (*string, bool)`

GetDrsAccessIdOk returns a tuple with the DrsAccessId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrsAccessId

`func (o *BulkAccessURL) SetDrsAccessId(v string)`

SetDrsAccessId sets DrsAccessId field to given value.

### HasDrsAccessId

`func (o *BulkAccessURL) HasDrsAccessId() bool`

HasDrsAccessId returns a boolean if a field has been set.

### GetUrl

`func (o *BulkAccessURL) GetUrl() string`

GetUrl returns the Url field if non-nil, zero value otherwise.

### GetUrlOk

`func (o *BulkAccessURL) GetUrlOk() (*string, bool)`

GetUrlOk returns a tuple with the Url field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUrl

`func (o *BulkAccessURL) SetUrl(v string)`

SetUrl sets Url field to given value.


### GetHeaders

`func (o *BulkAccessURL) GetHeaders() []string`

GetHeaders returns the Headers field if non-nil, zero value otherwise.

### GetHeadersOk

`func (o *BulkAccessURL) GetHeadersOk() (*[]string, bool)`

GetHeadersOk returns a tuple with the Headers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHeaders

`func (o *BulkAccessURL) SetHeaders(v []string)`

SetHeaders sets Headers field to given value.

### HasHeaders

`func (o *BulkAccessURL) HasHeaders() bool`

HasHeaders returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


