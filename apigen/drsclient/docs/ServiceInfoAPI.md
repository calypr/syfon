# \ServiceInfoAPI

All URIs are relative to *https://drs.example.org/ga4gh/drs/v1*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetServiceInfo**](ServiceInfoAPI.md#GetServiceInfo) | **Get** /service-info | Retrieve information about this service



## GetServiceInfo

> GetServiceInfo200Response GetServiceInfo(ctx).Execute()

Retrieve information about this service



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

	configuration := openapiclient.NewConfiguration()
	apiClient := openapiclient.NewAPIClient(configuration)
	resp, r, err := apiClient.ServiceInfoAPI.GetServiceInfo(context.Background()).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `ServiceInfoAPI.GetServiceInfo``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `GetServiceInfo`: GetServiceInfo200Response
	fmt.Fprintf(os.Stdout, "Response from `ServiceInfoAPI.GetServiceInfo`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetServiceInfoRequest struct via the builder pattern


### Return type

[**GetServiceInfo200Response**](GetServiceInfo200Response.md)

### Authorization

[BasicAuth](../README.md#BasicAuth), [BearerAuth](../README.md#BearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

