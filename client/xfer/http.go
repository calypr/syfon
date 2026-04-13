package xfer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

// DoUpload performs a presigned PUT request and returns ETag when available.
func DoUpload(ctx context.Context, req request.RequestInterface, url string, body io.Reader, size int64) (string, error) {
	rb := req.New(http.MethodPut, url).WithBody(body).WithSkipAuth(true)
	if size > 0 {
		rb.PartSize = size
	}

	resp, err := req.Do(ctx, rb)
	if err != nil {
		return "", fmt.Errorf("upload to %s failed: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload to %s failed with status %d: %s", url, resp.StatusCode, string(bodyBytes))
	}

	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

// GenericDownload performs GET (optionally ranged) against a signed URL.
func GenericDownload(ctx context.Context, req request.RequestInterface, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	skipAuth := common.IsCloudPresignedURL(signedURL)

	rb := req.New(http.MethodGet, signedURL)
	if rangeStart != nil {
		rangeHeader := "bytes=" + strconv.FormatInt(*rangeStart, 10) + "-"
		if rangeEnd != nil {
			rangeHeader += strconv.FormatInt(*rangeEnd, 10)
		}
		rb.WithHeader("Range", rangeHeader)
	}

	if skipAuth {
		rb.WithSkipAuth(true)
	}

	return req.Do(ctx, rb)
}
