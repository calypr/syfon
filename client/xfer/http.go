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

	method := http.MethodPut
	if parsed != nil && useGCSJSONMediaUpload(parsed) {
		method = http.MethodPost
	}

	skipAuth := common.IsCloudPresignedURL(urlStr)
	rb := req.New(method, urlStr).WithBody(body).WithTimeout(common.DataTimeout)
	if skipAuth {
		rb.WithSkipAuth(true)
	}
	if method == http.MethodPut && parsed != nil && needsAzureBlobTypeHeader(parsed) {
		// Azure Put Blob requires this header for SAS-based uploads.
		rb.WithHeader("x-ms-blob-type", "BlockBlob")
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
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload to %s failed with status %d: %s", urlStr, resp.StatusCode, string(bodyBytes))
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

func needsAzureBlobTypeHeader(parsed *url.URL) bool {
	if parsed == nil {
		return false
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return false
	}
	q := parsed.Query()
	if strings.TrimSpace(q.Get("comp")) != "" {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(q.Get("sr")), "b") {
		return false
	}
	return strings.TrimSpace(q.Get("sig")) != "" && strings.TrimSpace(q.Get("sv")) != ""
}

func useGCSJSONMediaUpload(parsed *url.URL) bool {
	if parsed == nil {
		return false
	}
	if strings.TrimSpace(parsed.Query().Get("uploadType")) != "media" {
		return false
	}
	if strings.TrimSpace(parsed.Query().Get("name")) == "" {
		return false
	}
	return strings.Contains(parsed.EscapedPath(), "/upload/storage/v1/b/")
}
