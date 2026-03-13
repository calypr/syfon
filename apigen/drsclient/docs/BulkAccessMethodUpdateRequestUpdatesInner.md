# BulkAccessMethodUpdateRequestUpdatesInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ObjectId** | **string** | DRS object ID to update | 
**AccessMethods** | [**[]AccessMethod**](AccessMethod.md) | New access methods for this object | 

## Methods

### NewBulkAccessMethodUpdateRequestUpdatesInner

`func NewBulkAccessMethodUpdateRequestUpdatesInner(objectId string, accessMethods []AccessMethod, ) *BulkAccessMethodUpdateRequestUpdatesInner`

NewBulkAccessMethodUpdateRequestUpdatesInner instantiates a new BulkAccessMethodUpdateRequestUpdatesInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBulkAccessMethodUpdateRequestUpdatesInnerWithDefaults

`func NewBulkAccessMethodUpdateRequestUpdatesInnerWithDefaults() *BulkAccessMethodUpdateRequestUpdatesInner`

NewBulkAccessMethodUpdateRequestUpdatesInnerWithDefaults instantiates a new BulkAccessMethodUpdateRequestUpdatesInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetObjectId

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) GetObjectId() string`

GetObjectId returns the ObjectId field if non-nil, zero value otherwise.

### GetObjectIdOk

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) GetObjectIdOk() (*string, bool)`

GetObjectIdOk returns a tuple with the ObjectId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetObjectId

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) SetObjectId(v string)`

SetObjectId sets ObjectId field to given value.


### GetAccessMethods

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) GetAccessMethods() []AccessMethod`

GetAccessMethods returns the AccessMethods field if non-nil, zero value otherwise.

### GetAccessMethodsOk

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) GetAccessMethodsOk() (*[]AccessMethod, bool)`

GetAccessMethodsOk returns a tuple with the AccessMethods field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAccessMethods

`func (o *BulkAccessMethodUpdateRequestUpdatesInner) SetAccessMethods(v []AccessMethod)`

SetAccessMethods sets AccessMethods field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


