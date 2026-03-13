# UploadRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Requests** | [**[]UploadRequestObject**](UploadRequestObject.md) | Array of upload requests for files | 
**Passports** | Pointer to **[]string** | Optional array of GA4GH Passport JWTs for authorization | [optional] 

## Methods

### NewUploadRequest

`func NewUploadRequest(requests []UploadRequestObject, ) *UploadRequest`

NewUploadRequest instantiates a new UploadRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUploadRequestWithDefaults

`func NewUploadRequestWithDefaults() *UploadRequest`

NewUploadRequestWithDefaults instantiates a new UploadRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetRequests

`func (o *UploadRequest) GetRequests() []UploadRequestObject`

GetRequests returns the Requests field if non-nil, zero value otherwise.

### GetRequestsOk

`func (o *UploadRequest) GetRequestsOk() (*[]UploadRequestObject, bool)`

GetRequestsOk returns a tuple with the Requests field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRequests

`func (o *UploadRequest) SetRequests(v []UploadRequestObject)`

SetRequests sets Requests field to given value.


### GetPassports

`func (o *UploadRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *UploadRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *UploadRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *UploadRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


