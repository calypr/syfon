package transfer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/request"
)

// DoUpload performs a presigned PUT request and returns ETag when available.
func DoUpload(ctx context.Context, client request.HTTPDoer, urlStr string, body io.Reader, size int64) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(urlStr))
	if err == nil && (parsed.Scheme == "" || strings.ToLower(parsed.Scheme) == "file") {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		dstPath := parsed.Path
		if dstPath == "" {
			dstPath = urlStr
		}
		if dstPath == "" {
			return "", fmt.Errorf("invalid file upload url: %s", redactUploadURL(urlStr))
		}
		if size < 0 {
			return "", fmt.Errorf("local upload size mismatch: declared size %d is negative", size)
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return "", fmt.Errorf("create upload target dir: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}

		dir := filepath.Dir(dstPath)
		stagingDir, err := os.MkdirTemp(dir, ".syfon-upload-*")
		if err != nil {
			return "", fmt.Errorf("create upload staging directory: %w", err)
		}
		defer os.Remove(stagingDir)
		tempPath := filepath.Join(stagingDir, "payload")
		f, err := os.OpenFile(tempPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if err != nil {
			return "", fmt.Errorf("create upload staging file: %w", err)
		}
		defer os.Remove(tempPath)

		var destinationMode os.FileMode
		preserveDestinationMode := false
		if info, statErr := os.Stat(dstPath); statErr == nil {
			if info.Mode().IsRegular() {
				destinationMode = info.Mode().Perm()
				preserveDestinationMode = true
			}
		} else if !os.IsNotExist(statErr) {
			_ = f.Close()
			return "", fmt.Errorf("stat upload target file: %w", statErr)
		}

		reader := uploadContextReader{ctx: ctx, reader: body}
		written, err := io.Copy(f, io.LimitReader(reader, size))
		if err != nil {
			_ = f.Close()
			return "", fmt.Errorf("read local upload body: %w", err)
		}
		if written != size {
			_ = f.Close()
			return "", fmt.Errorf("local upload size mismatch: wrote %d bytes, want %d", written, size)
		}

		extra, err := io.Copy(io.Discard, io.LimitReader(reader, 1))
		if err != nil {
			_ = f.Close()
			return "", fmt.Errorf("read local upload body: %w", err)
		}
		if extra != 0 {
			_ = f.Close()
			return "", fmt.Errorf("local upload size mismatch: body exceeds declared size %d", size)
		}
		if err := ctx.Err(); err != nil {
			_ = f.Close()
			return "", err
		}
		if preserveDestinationMode {
			if err := f.Chmod(destinationMode); err != nil {
				_ = f.Close()
				return "", fmt.Errorf("preserve upload target permissions: %w", err)
			}
		}
		if err := f.Close(); err != nil {
			return "", fmt.Errorf("close upload staging file: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := os.Rename(tempPath, dstPath); err != nil {
			return "", fmt.Errorf("replace upload target file: %w", err)
		}
		return "", nil
	}

	method := http.MethodPut
	if parsed != nil && useGCSJSONMediaUpload(parsed) {
		method = http.MethodPost
	}

	skipAuth := common.IsCloudPresignedURL(urlStr)
	ctx, cancel := context.WithTimeout(ctx, common.DataTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return "", fmt.Errorf("create upload request: %w", redactUploadError(err, urlStr))
	}
	if skipAuth {
		request.SkipAuth(req)
	}
	if method == http.MethodPut && parsed != nil && needsAzureBlobTypeHeader(parsed) {
		req.Header.Set("x-ms-blob-type", "BlockBlob")
	}
	if size > 0 {
		req.ContentLength = size
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("upload to %s failed: %w", redactUploadURL(urlStr), redactUploadError(err, urlStr))
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		err := common.ResponseBodyError(resp, fmt.Sprintf("upload to %s failed", redactUploadURL(urlStr)))
		return "", redactUploadError(err, urlStr)
	}

	return strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

type uploadContextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r uploadContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	n, err := r.reader.Read(p)
	if ctxErr := r.ctx.Err(); ctxErr != nil {
		return n, ctxErr
	}
	return n, err
}

type redactedUploadError struct {
	message string
	cause   error
}

func (e *redactedUploadError) Error() string {
	return e.message
}

func (e *redactedUploadError) Unwrap() error {
	return e.cause
}

func redactUploadError(err error, rawURL string) error {
	if err == nil {
		return nil
	}

	message := err.Error()
	safeURL := redactUploadURL(rawURL)
	for _, candidate := range []string{rawURL, strings.TrimSpace(rawURL)} {
		if candidate != "" {
			message = strings.ReplaceAll(message, candidate, safeURL)
		}
	}
	if parsed, parseErr := url.Parse(strings.TrimSpace(rawURL)); parseErr == nil {
		message = strings.ReplaceAll(message, parsed.String(), safeURL)
		if parsed.RawQuery != "" {
			message = strings.ReplaceAll(message, parsed.RawQuery, "[redacted]")
		}
	}
	if message == err.Error() {
		return err
	}
	return &redactedUploadError{message: message, cause: err}
}

func redactUploadURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		if queryStart := strings.IndexAny(rawURL, "?#"); queryStart >= 0 {
			return rawURL[:queryStart]
		}
		return rawURL
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	return parsed.String()
}

// GenericDownload performs GET (optionally ranged) against a signed URL.
func GenericDownload(ctx context.Context, client request.HTTPDoer, signedURL string, rangeStart, rangeEnd *int64) (*http.Response, error) {
	parsed, parseErr := url.Parse(strings.TrimSpace(signedURL))
	if parseErr == nil && (parsed.Scheme == "" || strings.ToLower(parsed.Scheme) == "file") {
		srcPath := parsed.Path
		if srcPath == "" {
			srcPath = signedURL
		}
		if srcPath == "" {
			return nil, fmt.Errorf("invalid file download url: %s", sanitizeDownloadURL(signedURL))
		}
		f, err := os.Open(srcPath)
		if err != nil {
			return nil, fmt.Errorf("open download source file: %w", err)
		}
		stat, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("stat download source file: %w", err)
		}

		reader := io.ReadCloser(f)
		status := http.StatusOK
		contentLength := stat.Size()
		header := make(http.Header)
		if rangeStart != nil {
			start := *rangeStart
			if start < 0 {
				start = 0
			}
			end := stat.Size() - 1
			if rangeEnd != nil && *rangeEnd >= 0 && *rangeEnd < end {
				end = *rangeEnd
			}
			if start > stat.Size() {
				start = stat.Size()
			}
			if end < start-1 {
				end = start - 1
			}
			length := int64(0)
			if end >= start {
				length = end - start + 1
			}
			reader = &sectionReadCloser{
				reader: io.NewSectionReader(f, start, length),
				closer: f,
			}
			status = http.StatusPartialContent
			contentLength = length
			if length > 0 {
				header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, stat.Size()))
			} else {
				status = http.StatusRequestedRangeNotSatisfiable
				header.Set("Content-Range", fmt.Sprintf("bytes */%d", stat.Size()))
			}
		}

		return &http.Response{
			StatusCode:    status,
			Header:        header,
			Body:          reader,
			ContentLength: contentLength,
		}, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create download request for %s: %w", sanitizeDownloadURL(signedURL), sanitizeDownloadError(err, signedURL))
	}
	if rangeStart != nil {
		rangeHeader := "bytes=" + strconv.FormatInt(*rangeStart, 10) + "-"
		if rangeEnd != nil {
			rangeHeader += strconv.FormatInt(*rangeEnd, 10)
		}
		req.Header.Set("Range", rangeHeader)
	}

	if common.IsCloudPresignedURL(signedURL) {
		request.SkipAuth(req)
	}

	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download from %s failed: %w", sanitizeDownloadURL(signedURL), sanitizeDownloadError(err, signedURL))
	}
	return response, nil
}

type sanitizedDownloadError struct {
	message string
	cause   error
}

func (e *sanitizedDownloadError) Error() string { return e.message }

func (e *sanitizedDownloadError) Unwrap() error { return e.cause }

func sanitizeDownloadError(err error, signedURL string) error {
	sanitized, _ := sanitizeDownloadErrorChain(err, signedURL)
	return sanitized
}

func sanitizeDownloadErrorChain(err error, signedURL string) (error, bool) {
	if err == nil {
		return nil, false
	}
	if requestErr, ok := err.(*url.Error); ok {
		sanitizedCause, causeChanged := sanitizeDownloadErrorChain(requestErr.Err, signedURL)
		sanitizedURL := sanitizeDownloadURL(requestErr.URL)
		if !causeChanged && sanitizedURL == requestErr.URL {
			return err, false
		}
		return &url.Error{Op: requestErr.Op, URL: sanitizedURL, Err: sanitizedCause}, true
	}

	message := sanitizeDownloadErrorText(err.Error(), signedURL)
	if many, ok := err.(interface{ Unwrap() []error }); ok {
		causes := many.Unwrap()
		sanitizedCauses := make([]error, len(causes))
		changed := message != err.Error()
		for i, cause := range causes {
			var causeChanged bool
			sanitizedCauses[i], causeChanged = sanitizeDownloadErrorChain(cause, signedURL)
			changed = changed || causeChanged
			if causeChanged && cause != nil && sanitizedCauses[i] != nil {
				message = strings.ReplaceAll(message, cause.Error(), sanitizedCauses[i].Error())
			}
		}
		if !changed {
			return err, false
		}
		return &sanitizedDownloadError{message: message, cause: errors.Join(sanitizedCauses...)}, true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		originalCause := wrapped.Unwrap()
		cause, causeChanged := sanitizeDownloadErrorChain(originalCause, signedURL)
		if causeChanged && originalCause != nil && cause != nil {
			message = strings.ReplaceAll(message, originalCause.Error(), cause.Error())
		}
		if !causeChanged && message == err.Error() {
			return err, false
		}
		return &sanitizedDownloadError{message: message, cause: cause}, true
	}
	if message != err.Error() {
		return &sanitizedDownloadError{message: message, cause: err}, true
	}
	return err, false
}

func sanitizeDownloadURL(rawURL string) string {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		if queryStart := strings.IndexAny(rawURL, "?#"); queryStart >= 0 {
			return rawURL[:queryStart]
		}
		return rawURL
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.ForceQuery = false
	parsed.Fragment = ""
	parsed.RawFragment = ""
	return parsed.String()
}

func sanitizeDownloadErrorText(message, rawURL string) string {
	safeURL := sanitizeDownloadURL(rawURL)
	for _, candidate := range []string{rawURL, strings.TrimSpace(rawURL)} {
		if candidate != "" {
			message = strings.ReplaceAll(message, candidate, safeURL)
		}
	}
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return message
	}
	message = strings.ReplaceAll(message, parsed.String(), safeURL)
	if parsed.User != nil {
		message = strings.ReplaceAll(message, parsed.User.String(), "[redacted]")
	}
	if parsed.RawQuery != "" {
		message = strings.ReplaceAll(message, parsed.RawQuery, "[redacted]")
	}
	if parsed.Fragment != "" {
		message = strings.ReplaceAll(message, parsed.Fragment, "[redacted]")
	}
	return message
}

type sectionReadCloser struct {
	reader   io.Reader
	closer   io.Closer
	closeMu  sync.Mutex
	closed   bool
	closeErr error
}

func (r *sectionReadCloser) Read(p []byte) (int, error) {
	return r.reader.Read(p)
}

func (r *sectionReadCloser) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.closer.Close()
	return r.closeErr
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
