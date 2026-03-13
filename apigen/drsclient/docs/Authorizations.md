# Authorizations

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**DrsObjectId** | Pointer to **string** |  | [optional] 
**SupportedTypes** | Pointer to **[]string** | An Optional list of support authorization types. More than one can be supported and tried in sequence. Defaults to &#x60;None&#x60; if empty or missing. | [optional] 
**PassportAuthIssuers** | Pointer to **[]string** | If authorizations contain &#x60;PassportAuth&#x60; this is a required list of visa issuers (as found in a visa&#39;s &#x60;iss&#x60; claim) that may authorize access to this object. The caller must only provide passports that contain visas from this list. It is strongly recommended that the caller validate that it is appropriate to send the requested passport/visa to the DRS server to mitigate attacks by malicious DRS servers requesting credentials they should not have. | [optional] 
**BearerAuthIssuers** | Pointer to **[]string** | If authorizations contain &#x60;BearerAuth&#x60; this is an optional list of issuers that may authorize access to this object. The caller must provide a token from one of these issuers. If this is empty or missing it assumed the caller knows which token to send via other means. It is strongly recommended that the caller validate that it is appropriate to send the requested token to the DRS server to mitigate attacks by malicious DRS servers requesting credentials they should not have. | [optional] 

## Methods

### NewAuthorizations

`func NewAuthorizations() *Authorizations`

NewAuthorizations instantiates a new Authorizations object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAuthorizationsWithDefaults

`func NewAuthorizationsWithDefaults() *Authorizations`

NewAuthorizationsWithDefaults instantiates a new Authorizations object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDrsObjectId

`func (o *Authorizations) GetDrsObjectId() string`

GetDrsObjectId returns the DrsObjectId field if non-nil, zero value otherwise.

### GetDrsObjectIdOk

`func (o *Authorizations) GetDrsObjectIdOk() (*string, bool)`

GetDrsObjectIdOk returns a tuple with the DrsObjectId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDrsObjectId

`func (o *Authorizations) SetDrsObjectId(v string)`

SetDrsObjectId sets DrsObjectId field to given value.

### HasDrsObjectId

`func (o *Authorizations) HasDrsObjectId() bool`

HasDrsObjectId returns a boolean if a field has been set.

### GetSupportedTypes

`func (o *Authorizations) GetSupportedTypes() []string`

GetSupportedTypes returns the SupportedTypes field if non-nil, zero value otherwise.

### GetSupportedTypesOk

`func (o *Authorizations) GetSupportedTypesOk() (*[]string, bool)`

GetSupportedTypesOk returns a tuple with the SupportedTypes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetSupportedTypes

`func (o *Authorizations) SetSupportedTypes(v []string)`

SetSupportedTypes sets SupportedTypes field to given value.

### HasSupportedTypes

`func (o *Authorizations) HasSupportedTypes() bool`

HasSupportedTypes returns a boolean if a field has been set.

### GetPassportAuthIssuers

`func (o *Authorizations) GetPassportAuthIssuers() []string`

GetPassportAuthIssuers returns the PassportAuthIssuers field if non-nil, zero value otherwise.

### GetPassportAuthIssuersOk

`func (o *Authorizations) GetPassportAuthIssuersOk() (*[]string, bool)`

GetPassportAuthIssuersOk returns a tuple with the PassportAuthIssuers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPassportAuthIssuers

`func (o *Authorizations) SetPassportAuthIssuers(v []string)`

SetPassportAuthIssuers sets PassportAuthIssuers field to given value.

### HasPassportAuthIssuers

`func (o *Authorizations) HasPassportAuthIssuers() bool`

HasPassportAuthIssuers returns a boolean if a field has been set.

### GetBearerAuthIssuers

`func (o *Authorizations) GetBearerAuthIssuers() []string`

GetBearerAuthIssuers returns the BearerAuthIssuers field if non-nil, zero value otherwise.

### GetBearerAuthIssuersOk

`func (o *Authorizations) GetBearerAuthIssuersOk() (*[]string, bool)`

GetBearerAuthIssuersOk returns a tuple with the BearerAuthIssuers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBearerAuthIssuers

`func (o *Authorizations) SetBearerAuthIssuers(v []string)`

SetBearerAuthIssuers sets BearerAuthIssuers field to given value.

### HasBearerAuthIssuers

`func (o *Authorizations) HasBearerAuthIssuers() bool`

HasBearerAuthIssuers returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


