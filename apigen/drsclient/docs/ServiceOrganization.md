# ServiceOrganization

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | Name of the organization responsible for the service | 
**Url** | **string** | URL of the website of the organization (RFC 3986 format) | 

## Methods

### NewServiceOrganization

`func NewServiceOrganization(name string, url string, ) *ServiceOrganization`

NewServiceOrganization instantiates a new ServiceOrganization object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewServiceOrganizationWithDefaults

`func NewServiceOrganizationWithDefaults() *ServiceOrganization`

NewServiceOrganizationWithDefaults instantiates a new ServiceOrganization object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *ServiceOrganization) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ServiceOrganization) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ServiceOrganization) SetName(v string)`

SetName sets Name field to given value.


### GetUrl

`func (o *ServiceOrganization) GetUrl() string`

GetUrl returns the Url field if non-nil, zero value otherwise.

### GetUrlOk

`func (o *ServiceOrganization) GetUrlOk() (*string, bool)`

GetUrlOk returns a tuple with the Url field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUrl

`func (o *ServiceOrganization) SetUrl(v string)`

SetUrl sets Url field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


