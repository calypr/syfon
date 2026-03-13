# AccessMethodAccessUrl

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Url** | **string** | A fully resolvable URL that can be used to fetch the actual object bytes. | 
**Headers** | Pointer to **[]string** | An optional list of headers to include in the HTTP request to &#x60;url&#x60;. These headers can be used to provide auth tokens required to fetch the object bytes. | [optional] 

## Methods

### NewAccessMethodAccessUrl

`func NewAccessMethodAccessUrl(url string, ) *AccessMethodAccessUrl`

NewAccessMethodAccessUrl instantiates a new AccessMethodAccessUrl object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccessMethodAccessUrlWithDefaults

`func NewAccessMethodAccessUrlWithDefaults() *AccessMethodAccessUrl`

NewAccessMethodAccessUrlWithDefaults instantiates a new AccessMethodAccessUrl object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetUrl

`func (o *AccessMethodAccessUrl) GetUrl() string`

GetUrl returns the Url field if non-nil, zero value otherwise.

### GetUrlOk

`func (o *AccessMethodAccessUrl) GetUrlOk() (*string, bool)`

GetUrlOk returns a tuple with the Url field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUrl

`func (o *AccessMethodAccessUrl) SetUrl(v string)`

SetUrl sets Url field to given value.


### GetHeaders

`func (o *AccessMethodAccessUrl) GetHeaders() []string`

GetHeaders returns the Headers field if non-nil, zero value otherwise.

### GetHeadersOk

`func (o *AccessMethodAccessUrl) GetHeadersOk() (*[]string, bool)`

GetHeadersOk returns a tuple with the Headers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHeaders

`func (o *AccessMethodAccessUrl) SetHeaders(v []string)`

SetHeaders sets Headers field to given value.

### HasHeaders

`func (o *AccessMethodAccessUrl) HasHeaders() bool`

HasHeaders returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


