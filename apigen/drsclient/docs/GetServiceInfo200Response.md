# GetServiceInfo200Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | Unique ID of this service. Reverse domain name notation is recommended, though not required. The identifier should attempt to be globally unique so it can be used in downstream aggregator services e.g. Service Registry. | 
**Name** | **string** | Name of this service. Should be human readable. | 
**Type** | [**DrsServiceType**](DrsServiceType.md) |  | 
**Description** | Pointer to **string** | Description of the service. Should be human readable and provide information about the service. | [optional] 
**Organization** | [**ServiceOrganization**](ServiceOrganization.md) |  | 
**ContactUrl** | Pointer to **string** | URL of the contact for the provider of this service, e.g. a link to a contact form (RFC 3986 format), or an email (RFC 2368 format). | [optional] 
**DocumentationUrl** | Pointer to **string** | URL of the documentation of this service (RFC 3986 format). This should help someone learn how to use your service, including any specifics required to access data, e.g. authentication. | [optional] 
**CreatedAt** | Pointer to **time.Time** | Timestamp describing when the service was first deployed and available (RFC 3339 format) | [optional] 
**UpdatedAt** | Pointer to **time.Time** | Timestamp describing when the service was last updated (RFC 3339 format) | [optional] 
**Environment** | Pointer to **string** | Environment the service is running in. Use this to distinguish between production, development and testing/staging deployments. Suggested values are prod, test, dev, staging. However this is advised and not enforced. | [optional] 
**Version** | **string** | Version of the service being described. Semantic versioning is recommended, but other identifiers, such as dates or commit hashes, are also allowed. The version should be changed whenever the service is updated. | 
**MaxBulkRequestLength** | **int32** | DEPRECATED - In 2.0 this will move to under the drs section of service info and not at the root level. The max length the bulk request endpoints can handle (&gt;&#x3D; 1) before generating a 413 error e.g. how long can the arrays bulk_object_ids and bulk_object_access_ids be for this server. | 
**Drs** | Pointer to [**DrsServiceDrs**](DrsServiceDrs.md) |  | [optional] 

## Methods

### NewGetServiceInfo200Response

`func NewGetServiceInfo200Response(id string, name string, type_ DrsServiceType, organization ServiceOrganization, version string, maxBulkRequestLength int32, ) *GetServiceInfo200Response`

NewGetServiceInfo200Response instantiates a new GetServiceInfo200Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetServiceInfo200ResponseWithDefaults

`func NewGetServiceInfo200ResponseWithDefaults() *GetServiceInfo200Response`

NewGetServiceInfo200ResponseWithDefaults instantiates a new GetServiceInfo200Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *GetServiceInfo200Response) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *GetServiceInfo200Response) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *GetServiceInfo200Response) SetId(v string)`

SetId sets Id field to given value.


### GetName

`func (o *GetServiceInfo200Response) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *GetServiceInfo200Response) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *GetServiceInfo200Response) SetName(v string)`

SetName sets Name field to given value.


### GetType

`func (o *GetServiceInfo200Response) GetType() DrsServiceType`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *GetServiceInfo200Response) GetTypeOk() (*DrsServiceType, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *GetServiceInfo200Response) SetType(v DrsServiceType)`

SetType sets Type field to given value.


### GetDescription

`func (o *GetServiceInfo200Response) GetDescription() string`

GetDescription returns the Description field if non-nil, zero value otherwise.

### GetDescriptionOk

`func (o *GetServiceInfo200Response) GetDescriptionOk() (*string, bool)`

GetDescriptionOk returns a tuple with the Description field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescription

`func (o *GetServiceInfo200Response) SetDescription(v string)`

SetDescription sets Description field to given value.

### HasDescription

`func (o *GetServiceInfo200Response) HasDescription() bool`

HasDescription returns a boolean if a field has been set.

### GetOrganization

`func (o *GetServiceInfo200Response) GetOrganization() ServiceOrganization`

GetOrganization returns the Organization field if non-nil, zero value otherwise.

### GetOrganizationOk

`func (o *GetServiceInfo200Response) GetOrganizationOk() (*ServiceOrganization, bool)`

GetOrganizationOk returns a tuple with the Organization field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrganization

`func (o *GetServiceInfo200Response) SetOrganization(v ServiceOrganization)`

SetOrganization sets Organization field to given value.


### GetContactUrl

`func (o *GetServiceInfo200Response) GetContactUrl() string`

GetContactUrl returns the ContactUrl field if non-nil, zero value otherwise.

### GetContactUrlOk

`func (o *GetServiceInfo200Response) GetContactUrlOk() (*string, bool)`

GetContactUrlOk returns a tuple with the ContactUrl field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetContactUrl

`func (o *GetServiceInfo200Response) SetContactUrl(v string)`

SetContactUrl sets ContactUrl field to given value.

### HasContactUrl

`func (o *GetServiceInfo200Response) HasContactUrl() bool`

HasContactUrl returns a boolean if a field has been set.

### GetDocumentationUrl

`func (o *GetServiceInfo200Response) GetDocumentationUrl() string`

GetDocumentationUrl returns the DocumentationUrl field if non-nil, zero value otherwise.

### GetDocumentationUrlOk

`func (o *GetServiceInfo200Response) GetDocumentationUrlOk() (*string, bool)`

GetDocumentationUrlOk returns a tuple with the DocumentationUrl field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDocumentationUrl

`func (o *GetServiceInfo200Response) SetDocumentationUrl(v string)`

SetDocumentationUrl sets DocumentationUrl field to given value.

### HasDocumentationUrl

`func (o *GetServiceInfo200Response) HasDocumentationUrl() bool`

HasDocumentationUrl returns a boolean if a field has been set.

### GetCreatedAt

`func (o *GetServiceInfo200Response) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *GetServiceInfo200Response) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *GetServiceInfo200Response) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.

### HasCreatedAt

`func (o *GetServiceInfo200Response) HasCreatedAt() bool`

HasCreatedAt returns a boolean if a field has been set.

### GetUpdatedAt

`func (o *GetServiceInfo200Response) GetUpdatedAt() time.Time`

GetUpdatedAt returns the UpdatedAt field if non-nil, zero value otherwise.

### GetUpdatedAtOk

`func (o *GetServiceInfo200Response) GetUpdatedAtOk() (*time.Time, bool)`

GetUpdatedAtOk returns a tuple with the UpdatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUpdatedAt

`func (o *GetServiceInfo200Response) SetUpdatedAt(v time.Time)`

SetUpdatedAt sets UpdatedAt field to given value.

### HasUpdatedAt

`func (o *GetServiceInfo200Response) HasUpdatedAt() bool`

HasUpdatedAt returns a boolean if a field has been set.

### GetEnvironment

`func (o *GetServiceInfo200Response) GetEnvironment() string`

GetEnvironment returns the Environment field if non-nil, zero value otherwise.

### GetEnvironmentOk

`func (o *GetServiceInfo200Response) GetEnvironmentOk() (*string, bool)`

GetEnvironmentOk returns a tuple with the Environment field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetEnvironment

`func (o *GetServiceInfo200Response) SetEnvironment(v string)`

SetEnvironment sets Environment field to given value.

### HasEnvironment

`func (o *GetServiceInfo200Response) HasEnvironment() bool`

HasEnvironment returns a boolean if a field has been set.

### GetVersion

`func (o *GetServiceInfo200Response) GetVersion() string`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *GetServiceInfo200Response) GetVersionOk() (*string, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *GetServiceInfo200Response) SetVersion(v string)`

SetVersion sets Version field to given value.


### GetMaxBulkRequestLength

`func (o *GetServiceInfo200Response) GetMaxBulkRequestLength() int32`

GetMaxBulkRequestLength returns the MaxBulkRequestLength field if non-nil, zero value otherwise.

### GetMaxBulkRequestLengthOk

`func (o *GetServiceInfo200Response) GetMaxBulkRequestLengthOk() (*int32, bool)`

GetMaxBulkRequestLengthOk returns a tuple with the MaxBulkRequestLength field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMaxBulkRequestLength

`func (o *GetServiceInfo200Response) SetMaxBulkRequestLength(v int32)`

SetMaxBulkRequestLength sets MaxBulkRequestLength field to given value.


### GetDrs

`func (o *GetServiceInfo200Response) GetDrs() DrsServiceDrs`

GetDrs returns the Drs field if non-nil, zero value otherwise.

### GetDrsOk

`func (o *GetServiceInfo200Response) GetDrsOk() (*DrsServiceDrs, bool)`

GetDrsOk returns a tuple with the Drs field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrs

`func (o *GetServiceInfo200Response) SetDrs(v DrsServiceDrs)`

SetDrs sets Drs field to given value.

### HasDrs

`func (o *GetServiceInfo200Response) HasDrs() bool`

HasDrs returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


