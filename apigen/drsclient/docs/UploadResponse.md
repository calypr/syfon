# UploadResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Responses** | [**[]UploadResponseObject**](UploadResponseObject.md) | List of upload responses for the requested files | 

## Methods

### NewUploadResponse

`func NewUploadResponse(responses []UploadResponseObject, ) *UploadResponse`

NewUploadResponse instantiates a new UploadResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewUploadResponseWithDefaults

`func NewUploadResponseWithDefaults() *UploadResponse`

NewUploadResponseWithDefaults instantiates a new UploadResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetResponses

`func (o *UploadResponse) GetResponses() []UploadResponseObject`

GetResponses returns the Responses field if non-nil, zero value otherwise.

### GetResponsesOk

`func (o *UploadResponse) GetResponsesOk() (*[]UploadResponseObject, bool)`

GetResponsesOk returns a tuple with the Responses field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetResponses

`func (o *UploadResponse) SetResponses(v []UploadResponseObject)`

SetResponses sets Responses field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


