# \ObjectsAPI

All URIs are relative to *https://drs.example.org/ga4gh/drs/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**BulkDeleteObjects**](ObjectsAPI.md#BulkDeleteObjects) | **Post** /objects/delete | Delete multiple DRS objects
[**BulkUpdateAccessMethods**](ObjectsAPI.md#BulkUpdateAccessMethods) | **Post** /objects/access-methods | Bulk update access methods for multiple DRS objects
[**DeleteObject**](ObjectsAPI.md#DeleteObject) | **Post** /objects/{object_id}/delete | Delete a DRS object (optional endpoint)
[**GetAccessURL**](ObjectsAPI.md#GetAccessURL) | **Get** /objects/{object_id}/access/{access_id} | Get a URL for fetching bytes
[**GetBulkAccessURL**](ObjectsAPI.md#GetBulkAccessURL) | **Post** /objects/access | Get URLs for fetching bytes from multiple objects with an optional Passport(s).
[**GetBulkObjects**](ObjectsAPI.md#GetBulkObjects) | **Post** /objects | Get info about multiple DrsObjects with an optional Passport(s).
[**GetObject**](ObjectsAPI.md#GetObject) | **Get** /objects/{object_id} | Get info about a DrsObject.
[**GetObjectsByChecksum**](ObjectsAPI.md#GetObjectsByChecksum) | **Get** /objects/checksum/{checksum} | Get DRS objects that are a match for the checksum.
[**OptionsBulkObject**](ObjectsAPI.md#OptionsBulkObject) | **Options** /objects | Get Authorization info about multiple DrsObjects.
[**OptionsObject**](ObjectsAPI.md#OptionsObject) | **Options** /objects/{object_id} | Get Authorization info about a DrsObject.
[**PostAccessURL**](ObjectsAPI.md#PostAccessURL) | **Post** /objects/{object_id}/access/{access_id} | Get a URL for fetching bytes through POST&#39;ing a Passport
[**PostObject**](ObjectsAPI.md#PostObject) | **Post** /objects/{object_id} | Get info about a DrsObject through POST&#39;ing a Passport.
[**RegisterObjects**](ObjectsAPI.md#RegisterObjects) | **Post** /objects/register | Register DRS objects
[**UpdateObjectAccessMethods**](ObjectsAPI.md#UpdateObjectAccessMethods) | **Post** /objects/{object_id}/access-methods | Update access methods for a DRS object



## BulkDeleteObjects

> BulkDeleteObjects(ctx).Body(body).Execute()

Delete multiple DRS objects



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	body := *openapiclient.NewBulkDeleteRequest([]string{"BulkObjectIds_example"}) // BulkDeleteRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.ObjectsAPI.BulkDeleteObjects(context.Background()).Body(body).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.BulkDeleteObjects``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiBulkDeleteObjectsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**BulkDeleteRequest**](BulkDeleteRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## BulkUpdateAccessMethods

> BulkUpdateAccessMethods200Response BulkUpdateAccessMethods(ctx).BulkAccessMethodUpdateRequest(bulkAccessMethodUpdateRequest).Execute()

Bulk update access methods for multiple DRS objects



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	bulkAccessMethodUpdateRequest := *openapiclient.NewBulkAccessMethodUpdateRequest([]openapiclient.BulkAccessMethodUpdateRequestUpdatesInner{*openapiclient.NewBulkAccessMethodUpdateRequestUpdatesInner("ObjectId_example", []openapiclient.AccessMethod{*openapiclient.NewAccessMethod("Type_example")})}) // BulkAccessMethodUpdateRequest | Request body for bulk updating access methods of multiple DRS objects

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.BulkUpdateAccessMethods(context.Background()).BulkAccessMethodUpdateRequest(bulkAccessMethodUpdateRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.BulkUpdateAccessMethods``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `BulkUpdateAccessMethods`: BulkUpdateAccessMethods200Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.BulkUpdateAccessMethods`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiBulkUpdateAccessMethodsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bulkAccessMethodUpdateRequest** | [**BulkAccessMethodUpdateRequest**](BulkAccessMethodUpdateRequest.md) | Request body for bulk updating access methods of multiple DRS objects | 

### Return type

[**BulkUpdateAccessMethods200Response**](BulkUpdateAccessMethods200Response.md)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteObject

> DeleteObject(ctx, objectId).Body(body).Execute()

Delete a DRS object (optional endpoint)



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier
	body := *openapiclient.NewDeleteRequest() // DeleteRequest |  (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	r, err := apiClient.ObjectsAPI.DeleteObject(context.Background(), objectId).Body(body).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.DeleteObject``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteObjectRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **body** | [**DeleteRequest**](DeleteRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetAccessURL

> AccessURL GetAccessURL(ctx, objectId, accessId).Execute()

Get a URL for fetching bytes



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier
	accessId := "accessId_example" // string | An `access_id` from the `access_methods` list of a `DrsObject`

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.GetAccessURL(context.Background(), objectId, accessId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.GetAccessURL``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetAccessURL`: AccessURL
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.GetAccessURL`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 
**accessId** | **string** | An &#x60;access_id&#x60; from the &#x60;access_methods&#x60; list of a &#x60;DrsObject&#x60; | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetAccessURLRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------



### Return type

[**AccessURL**](AccessURL.md)

### Authorization

[BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetBulkAccessURL

> GetBulkAccessURL200Response GetBulkAccessURL(ctx).BulkObjectAccessId(bulkObjectAccessId).Execute()

Get URLs for fetching bytes from multiple objects with an optional Passport(s).



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	bulkObjectAccessId := *openapiclient.NewBulkObjectAccessId() // BulkObjectAccessId | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.GetBulkAccessURL(context.Background()).BulkObjectAccessId(bulkObjectAccessId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.GetBulkAccessURL``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetBulkAccessURL`: GetBulkAccessURL200Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.GetBulkAccessURL`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetBulkAccessURLRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bulkObjectAccessId** | [**BulkObjectAccessId**](BulkObjectAccessId.md) |  | 

### Return type

[**GetBulkAccessURL200Response**](GetBulkAccessURL200Response.md)

### Authorization

[PassportAuth](../README.md#PassportAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetBulkObjects

> GetBulkObjects200Response GetBulkObjects(ctx).GetBulkObjectsRequest(getBulkObjectsRequest).Expand(expand).Execute()

Get info about multiple DrsObjects with an optional Passport(s).



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	getBulkObjectsRequest := *openapiclient.NewGetBulkObjectsRequest([]string{"BulkObjectIds_example"}) // GetBulkObjectsRequest | 
	expand := false // bool | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains other bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.GetBulkObjects(context.Background()).GetBulkObjectsRequest(getBulkObjectsRequest).Expand(expand).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.GetBulkObjects``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetBulkObjects`: GetBulkObjects200Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.GetBulkObjects`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetBulkObjectsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **getBulkObjectsRequest** | [**GetBulkObjectsRequest**](GetBulkObjectsRequest.md) |  | 
 **expand** | **bool** | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains other bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. | 

### Return type

[**GetBulkObjects200Response**](GetBulkObjects200Response.md)

### Authorization

[PassportAuth](../README.md#PassportAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetObject

> DrsObject GetObject(ctx, objectId).Expand(expand).Execute()

Get info about a DrsObject.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier
	expand := false // bool | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains other bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. (optional)

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.GetObject(context.Background(), objectId).Expand(expand).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.GetObject``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetObject`: DrsObject
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.GetObject`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetObjectRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **expand** | **bool** | If false and the object_id refers to a bundle, then the ContentsObject array contains only those objects directly contained in the bundle. That is, if the bundle contains other bundles, those other bundles are not recursively included in the result. If true and the object_id refers to a bundle, then the entire set of objects in the bundle is expanded. That is, if the bundle contains other bundles, then those other bundles are recursively expanded and included in the result. Recursion continues through the entire sub-tree of the bundle. If the object_id refers to a blob, then the query parameter is ignored. | 

### Return type

[**DrsObject**](DrsObject.md)

### Authorization

[BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetObjectsByChecksum

> GetBulkObjects200Response GetObjectsByChecksum(ctx, checksum).Execute()

Get DRS objects that are a match for the checksum.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	checksum := "checksum_example" // string | A `checksum` value from the `checksums` list of a `DrsObject`

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.GetObjectsByChecksum(context.Background(), checksum).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.GetObjectsByChecksum``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetObjectsByChecksum`: GetBulkObjects200Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.GetObjectsByChecksum`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**checksum** | **string** | A &#x60;checksum&#x60; value from the &#x60;checksums&#x60; list of a &#x60;DrsObject&#x60; | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetObjectsByChecksumRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**GetBulkObjects200Response**](GetBulkObjects200Response.md)

### Authorization

[PassportAuth](../README.md#PassportAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## OptionsBulkObject

> OptionsBulkObject200Response OptionsBulkObject(ctx).BulkObjectIdNoPassport(bulkObjectIdNoPassport).Execute()

Get Authorization info about multiple DrsObjects.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	bulkObjectIdNoPassport := *openapiclient.NewBulkObjectIdNoPassport() // BulkObjectIdNoPassport | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.OptionsBulkObject(context.Background()).BulkObjectIdNoPassport(bulkObjectIdNoPassport).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.OptionsBulkObject``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `OptionsBulkObject`: OptionsBulkObject200Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.OptionsBulkObject`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiOptionsBulkObjectRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **bulkObjectIdNoPassport** | [**BulkObjectIdNoPassport**](BulkObjectIdNoPassport.md) |  | 

### Return type

[**OptionsBulkObject200Response**](OptionsBulkObject200Response.md)

### Authorization

[BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## OptionsObject

> Authorizations OptionsObject(ctx, objectId).Execute()

Get Authorization info about a DrsObject.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.OptionsObject(context.Background(), objectId).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.OptionsObject``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `OptionsObject`: Authorizations
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.OptionsObject`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 

### Other Parameters

Other parameters are passed through a pointer to a apiOptionsObjectRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**Authorizations**](Authorizations.md)

### Authorization

[BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PostAccessURL

> AccessURL PostAccessURL(ctx, objectId, accessId).PostAccessURLRequest(postAccessURLRequest).Execute()

Get a URL for fetching bytes through POST'ing a Passport



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier
	accessId := "accessId_example" // string | An `access_id` from the `access_methods` list of a `DrsObject`
	postAccessURLRequest := *openapiclient.NewPostAccessURLRequest() // PostAccessURLRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.PostAccessURL(context.Background(), objectId, accessId).PostAccessURLRequest(postAccessURLRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.PostAccessURL``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `PostAccessURL`: AccessURL
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.PostAccessURL`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 
**accessId** | **string** | An &#x60;access_id&#x60; from the &#x60;access_methods&#x60; list of a &#x60;DrsObject&#x60; | 

### Other Parameters

Other parameters are passed through a pointer to a apiPostAccessURLRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


 **postAccessURLRequest** | [**PostAccessURLRequest**](PostAccessURLRequest.md) |  | 

### Return type

[**AccessURL**](AccessURL.md)

### Authorization

[PassportAuth](../README.md#PassportAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PostObject

> DrsObject PostObject(ctx, objectId).PostObjectRequest(postObjectRequest).Execute()

Get info about a DrsObject through POST'ing a Passport.



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | `DrsObject` identifier
	postObjectRequest := *openapiclient.NewPostObjectRequest() // PostObjectRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.PostObject(context.Background(), objectId).PostObjectRequest(postObjectRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.PostObject``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `PostObject`: DrsObject
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.PostObject`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | &#x60;DrsObject&#x60; identifier | 

### Other Parameters

Other parameters are passed through a pointer to a apiPostObjectRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **postObjectRequest** | [**PostObjectRequest**](PostObjectRequest.md) |  | 

### Return type

[**DrsObject**](DrsObject.md)

### Authorization

[PassportAuth](../README.md#PassportAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## RegisterObjects

> RegisterObjects201Response RegisterObjects(ctx).Body(body).Execute()

Register DRS objects



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	body := *openapiclient.NewRegisterObjectsRequest([]openapiclient.DrsObjectCandidate{*openapiclient.NewDrsObjectCandidate(int64(123), []openapiclient.Checksum{*openapiclient.NewChecksum("Checksum_example", "sha-256")})}) // RegisterObjectsRequest | Request body for registering DRS objects after upload

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.RegisterObjects(context.Background()).Body(body).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.RegisterObjects``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `RegisterObjects`: RegisterObjects201Response
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.RegisterObjects`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiRegisterObjectsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**RegisterObjectsRequest**](RegisterObjectsRequest.md) | Request body for registering DRS objects after upload | 

### Return type

[**RegisterObjects201Response**](RegisterObjects201Response.md)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## UpdateObjectAccessMethods

> DrsObject UpdateObjectAccessMethods(ctx, objectId).AccessMethodUpdateRequest(accessMethodUpdateRequest).Execute()

Update access methods for a DRS object



### Example

```go
package main

import (
	"context"
	"fmt"
	"os"
	openapiclient "github.com/calypr/drs-server/drsclient"
)

func main() {
	objectId := "objectId_example" // string | DRS object identifier
	accessMethodUpdateRequest := *openapiclient.NewAccessMethodUpdateRequest([]openapiclient.AccessMethod{*openapiclient.NewAccessMethod("Type_example")}) // AccessMethodUpdateRequest | Request body for updating access methods of a DRS object

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ObjectsAPI.UpdateObjectAccessMethods(context.Background(), objectId).AccessMethodUpdateRequest(accessMethodUpdateRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ObjectsAPI.UpdateObjectAccessMethods``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `UpdateObjectAccessMethods`: DrsObject
	fmt.Fprintf(os.Stdout, "Response from `ObjectsAPI.UpdateObjectAccessMethods`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**objectId** | **string** | DRS object identifier | 

### Other Parameters

Other parameters are passed through a pointer to a apiUpdateObjectAccessMethodsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **accessMethodUpdateRequest** | [**AccessMethodUpdateRequest**](AccessMethodUpdateRequest.md) | Request body for updating access methods of a DRS object | 

### Return type

[**DrsObject**](DrsObject.md)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

