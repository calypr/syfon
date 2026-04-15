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
func DoUpload(ctx context.Context, req request.Requester, urlStr string, body io.Reader, size int64) (string, error) {
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
	opts := []request.RequestOption{
		request.WithTimeout(common.DataTimeout),
	}
	if skipAuth {
		opts = append(opts, request.WithSkipAuth(true))
	}
	if size > 0 {
		opts = append(opts, request.WithPartSize(size))
	}

	var resp *http.Response
	err = req.Do(ctx, http.MethodPut, urlStr, body, &resp, opts...)
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
func GenericDownload(ctx context.Context, req request.Requester, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	skipAuth := common.IsCloudPresignedURL(signedURL)

	opts := []request.RequestOption{}
	if rangeStart != nil {
		rangeHeader := "bytes=" + strconv.FormatInt(*rangeStart, 10) + "-"
		if rangeEnd != nil {
			rangeHeader += strconv.FormatInt(*rangeEnd, 10)
		}
		opts = append(opts, request.WithHeader("Range", rangeHeader))
	}

	if skipAuth {
		opts = append(opts, request.WithSkipAuth(true))
	}

	var resp *http.Response
	err := req.Do(ctx, http.MethodGet, signedURL, nil, &resp, opts...)
	return resp, err
}
