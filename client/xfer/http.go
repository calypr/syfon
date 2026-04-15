package xfer

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

// DoUpload performs a presigned PUT request and returns ETag when available.
func DoUpload(ctx context.Context, req request.RequestInterface, urlStr string, body io.Reader, size int64) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(urlStr))
	if err == nil && strings.ToLower(parsed.Scheme) == "file" {
		dstPath := parsed.Path
		if dstPath == "" {
			return "", fmt.Errorf("invalid file upload url: %s", urlStr)
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return "", fmt.Errorf("create upload target dir: %w", err)
		}
		f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return "", fmt.Errorf("open upload target file: %w", err)
		}
		defer f.Close()
		_, err = io.Copy(f, body)
		return "", err
	}

	skipAuth := common.IsCloudPresignedURL(urlStr)
	rb := req.New(http.MethodPut, urlStr).WithBody(body).WithTimeout(common.DataTimeout)
	if skipAuth {
		rb.WithSkipAuth(true)
	}
	if size > 0 {
		rb.PartSize = size
	}

	resp, err := req.Do(ctx, rb)
	if err != nil {
		return "", fmt.Errorf("upload to %s failed: %w", urlStr, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", common.ResponseBodyError(resp, fmt.Sprintf("upload to %s failed", urlStr))
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
