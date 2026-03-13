# \UploadRequestAPI

All URIs are relative to *https://drs.example.org/ga4gh/drs/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**PostUploadRequest**](UploadRequestAPI.md#PostUploadRequest) | **Post** /upload-request | Request upload methods for files



## PostUploadRequest

> UploadResponse PostUploadRequest(ctx).UploadRequest(uploadRequest).Execute()

Request upload methods for files



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
	uploadRequest := *openapiclient.NewUploadRequest([]openapiclient.UploadRequestObject{*openapiclient.NewUploadRequestObject("Name_example", int64(123), "MimeType_example", []openapiclient.Checksum{*openapiclient.NewChecksum("Checksum_example", "sha-256")})}) // UploadRequest | 

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.UploadRequestAPI.PostUploadRequest(context.Background()).UploadRequest(uploadRequest).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `UploadRequestAPI.PostUploadRequest``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `PostUploadRequest`: UploadResponse
	fmt.Fprintf(os.Stdout, "Response from `UploadRequestAPI.PostUploadRequest`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiPostUploadRequestRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **uploadRequest** | [**UploadRequest**](UploadRequest.md) |  | 

### Return type

[**UploadResponse**](UploadResponse.md)

### Authorization

[PassportAuth](../README.md#PassportAuth), [BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

