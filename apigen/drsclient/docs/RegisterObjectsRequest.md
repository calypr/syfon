# RegisterObjectsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Candidates** | [**[]DrsObjectCandidate**](DrsObjectCandidate.md) | Array of DRS object candidates to register (server will mint IDs and timestamps) | 
**Passports** | Pointer to **[]string** | Optional array of GA4GH Passport JWTs for authorization | [optional] 

## Methods

### NewRegisterObjectsRequest

`func NewRegisterObjectsRequest(candidates []DrsObjectCandidate, ) *RegisterObjectsRequest`

NewRegisterObjectsRequest instantiates a new RegisterObjectsRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewRegisterObjectsRequestWithDefaults

`func NewRegisterObjectsRequestWithDefaults() *RegisterObjectsRequest`

NewRegisterObjectsRequestWithDefaults instantiates a new RegisterObjectsRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCandidates

`func (o *RegisterObjectsRequest) GetCandidates() []DrsObjectCandidate`

GetCandidates returns the Candidates field if non-nil, zero value otherwise.

### GetCandidatesOk

`func (o *RegisterObjectsRequest) GetCandidatesOk() (*[]DrsObjectCandidate, bool)`

GetCandidatesOk returns a tuple with the Candidates field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCandidates

`func (o *RegisterObjectsRequest) SetCandidates(v []DrsObjectCandidate)`

SetCandidates sets Candidates field to given value.


### GetPassports

`func (o *RegisterObjectsRequest) GetPassports() []string`

GetPassports returns the Passports field if non-nil, zero value otherwise.

### GetPassportsOk

`func (o *RegisterObjectsRequest) GetPassportsOk() (*[]string, bool)`

GetPassportsOk returns a tuple with the Passports field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassports

`func (o *RegisterObjectsRequest) SetPassports(v []string)`

SetPassports sets Passports field to given value.

### HasPassports

`func (o *RegisterObjectsRequest) HasPassports() bool`

HasPassports returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


