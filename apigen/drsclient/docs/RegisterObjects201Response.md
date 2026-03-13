# RegisterObjects201Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Objects** | [**[]DrsObject**](DrsObject.md) | Array of registered DRS objects in the same order as the candidates in the request | 

## Methods

### NewRegisterObjects201Response

`func NewRegisterObjects201Response(objects []DrsObject, ) *RegisterObjects201Response`

NewRegisterObjects201Response instantiates a new RegisterObjects201Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewRegisterObjects201ResponseWithDefaults

`func NewRegisterObjects201ResponseWithDefaults() *RegisterObjects201Response`

NewRegisterObjects201ResponseWithDefaults instantiates a new RegisterObjects201Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetObjects

`func (o *RegisterObjects201Response) GetObjects() []DrsObject`

GetObjects returns the Objects field if non-nil, zero value otherwise.

### GetObjectsOk

`func (o *RegisterObjects201Response) GetObjectsOk() (*[]DrsObject, bool)`

GetObjectsOk returns a tuple with the Objects field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetObjects

`func (o *RegisterObjects201Response) SetObjects(v []DrsObject)`

SetObjects sets Objects field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


