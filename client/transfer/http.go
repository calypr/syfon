package transfer

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

// ResolveRange parses range information from FileDownloadResponseObject.
func ResolveRange(fdr *common.FileDownloadResponseObject) (start int64, end *int64, ok bool) {
	if fdr == nil {
		return 0, nil, false
	}
	if fdr.RangeStart != nil {
		return *fdr.RangeStart, fdr.RangeEnd, true
	}
	if fdr.Range > 0 {
		return fdr.Range, nil, true
	}
	return 0, nil, false
}

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
func GenericDownload(ctx context.Context, req request.RequestInterface, fdr *common.FileDownloadResponseObject) (*http.Response, error) {
	skipAuth := common.IsCloudPresignedURL(fdr.PresignedURL)

	rb := req.New(http.MethodGet, fdr.PresignedURL)
	start, end, hasRange := ResolveRange(fdr)
	if hasRange {
		rangeHeader := "bytes=" + strconv.FormatInt(start, 10) + "-"
		if end != nil {
			rangeHeader += strconv.FormatInt(*end, 10)
		}
		rb.WithHeader("Range", rangeHeader)
	}

	if skipAuth {
		rb.WithSkipAuth(true)
	}

	return req.Do(ctx, rb)
}
