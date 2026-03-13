# Checksum

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Checksum** | **string** | The hex-string encoded checksum for the data | 
**Type** | **string** | The digest method used to create the checksum. The value (e.g. &#x60;sha-256&#x60;) SHOULD be listed as &#x60;Hash Name String&#x60; in the https://www.iana.org/assignments/named-information/named-information.xhtml#hash-alg[IANA Named Information Hash Algorithm Registry]. Other values MAY be used, as long as implementors are aware of the issues discussed in https://tools.ietf.org/html/rfc6920#section-9.4[RFC6920]. GA4GH may provide more explicit guidance for use of non-IANA-registered algorithms in the future. Until then, if implementers do choose such an algorithm (e.g. because it&#39;s implemented by their storage provider), they SHOULD use an existing standard &#x60;type&#x60; value such as &#x60;md5&#x60;, &#x60;etag&#x60;, &#x60;crc32c&#x60;, &#x60;trunc512&#x60;, or &#x60;sha1&#x60;. | 

## Methods

### NewChecksum

`func NewChecksum(checksum string, type_ string, ) *Checksum`

NewChecksum instantiates a new Checksum object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewChecksumWithDefaults

`func NewChecksumWithDefaults() *Checksum`

NewChecksumWithDefaults instantiates a new Checksum object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetChecksum

`func (o *Checksum) GetChecksum() string`

GetChecksum returns the Checksum field if non-nil, zero value otherwise.

### GetChecksumOk

`func (o *Checksum) GetChecksumOk() (*string, bool)`

GetChecksumOk returns a tuple with the Checksum field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetChecksum

`func (o *Checksum) SetChecksum(v string)`

SetChecksum sets Checksum field to given value.


### GetType

`func (o *Checksum) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *Checksum) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *Checksum) SetType(v string)`

SetType sets Type field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


