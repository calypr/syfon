# AccessMethod

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Type** | **string** | Type of the access method. | 
**AccessUrl** | Pointer to [**AccessMethodAccessUrl**](AccessMethodAccessUrl.md) |  | [optional] 
**AccessId** | Pointer to **string** | An arbitrary string to be passed to the &#x60;/access&#x60; method to get an &#x60;AccessURL&#x60;. This string must be unique within the scope of a single object. Note that at least one of &#x60;access_url&#x60; and &#x60;access_id&#x60; must be provided. | [optional] 
**Cloud** | Pointer to **string** | Name of the cloud service provider that the object belongs to. If the cloud service is Amazon Web Services, Google Cloud Platform or Azure the values should be &#x60;aws&#x60;, &#x60;gcp&#x60;, or &#x60;azure&#x60; respectively. | [optional] 
**Region** | Pointer to **string** | Name of the region in the cloud service provider that the object belongs to. | [optional] 
**Available** | Pointer to **bool** | Availablity of file in the cloud. This label defines if this file is immediately accessible via DRS. Any delay or requirement of thawing mechanism if the file is in offline/archival storage is classified as false, meaning it is unavailable. | [optional] 
**Authorizations** | Pointer to [**AccessMethodAuthorizations**](AccessMethodAuthorizations.md) |  | [optional] 

## Methods

### NewAccessMethod

`func NewAccessMethod(type_ string, ) *AccessMethod`

NewAccessMethod instantiates a new AccessMethod object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccessMethodWithDefaults

`func NewAccessMethodWithDefaults() *AccessMethod`

NewAccessMethodWithDefaults instantiates a new AccessMethod object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetType

`func (o *AccessMethod) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *AccessMethod) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *AccessMethod) SetType(v string)`

SetType sets Type field to given value.


### GetAccessUrl

`func (o *AccessMethod) GetAccessUrl() AccessMethodAccessUrl`

GetAccessUrl returns the AccessUrl field if non-nil, zero value otherwise.

### GetAccessUrlOk

`func (o *AccessMethod) GetAccessUrlOk() (*AccessMethodAccessUrl, bool)`

GetAccessUrlOk returns a tuple with the AccessUrl field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessUrl

`func (o *AccessMethod) SetAccessUrl(v AccessMethodAccessUrl)`

SetAccessUrl sets AccessUrl field to given value.

### HasAccessUrl

`func (o *AccessMethod) HasAccessUrl() bool`

HasAccessUrl returns a boolean if a field has been set.

### GetAccessId

`func (o *AccessMethod) GetAccessId() string`

GetAccessId returns the AccessId field if non-nil, zero value otherwise.

### GetAccessIdOk

`func (o *AccessMethod) GetAccessIdOk() (*string, bool)`

GetAccessIdOk returns a tuple with the AccessId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessId

`func (o *AccessMethod) SetAccessId(v string)`

SetAccessId sets AccessId field to given value.

### HasAccessId

`func (o *AccessMethod) HasAccessId() bool`

HasAccessId returns a boolean if a field has been set.

### GetCloud

`func (o *AccessMethod) GetCloud() string`

GetCloud returns the Cloud field if non-nil, zero value otherwise.

### GetCloudOk

`func (o *AccessMethod) GetCloudOk() (*string, bool)`

GetCloudOk returns a tuple with the Cloud field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCloud

`func (o *AccessMethod) SetCloud(v string)`

SetCloud sets Cloud field to given value.

### HasCloud

`func (o *AccessMethod) HasCloud() bool`

HasCloud returns a boolean if a field has been set.

### GetRegion

`func (o *AccessMethod) GetRegion() string`

GetRegion returns the Region field if non-nil, zero value otherwise.

### GetRegionOk

`func (o *AccessMethod) GetRegionOk() (*string, bool)`

GetRegionOk returns a tuple with the Region field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRegion

`func (o *AccessMethod) SetRegion(v string)`

SetRegion sets Region field to given value.

### HasRegion

`func (o *AccessMethod) HasRegion() bool`

HasRegion returns a boolean if a field has been set.

### GetAvailable

`func (o *AccessMethod) GetAvailable() bool`

GetAvailable returns the Available field if non-nil, zero value otherwise.

### GetAvailableOk

`func (o *AccessMethod) GetAvailableOk() (*bool, bool)`

GetAvailableOk returns a tuple with the Available field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAvailable

`func (o *AccessMethod) SetAvailable(v bool)`

SetAvailable sets Available field to given value.

### HasAvailable

`func (o *AccessMethod) HasAvailable() bool`

HasAvailable returns a boolean if a field has been set.

### GetAuthorizations

`func (o *AccessMethod) GetAuthorizations() AccessMethodAuthorizations`

GetAuthorizations returns the Authorizations field if non-nil, zero value otherwise.

### GetAuthorizationsOk

`func (o *AccessMethod) GetAuthorizationsOk() (*AccessMethodAuthorizations, bool)`

GetAuthorizationsOk returns a tuple with the Authorizations field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAuthorizations

`func (o *AccessMethod) SetAuthorizations(v AccessMethodAuthorizations)`

SetAuthorizations sets Authorizations field to given value.

### HasAuthorizations

`func (o *AccessMethod) HasAuthorizations() bool`

HasAuthorizations returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


