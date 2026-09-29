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
	"runtime"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/request"
)

type captureRequester struct {
	method  string
	path    string
	request *http.Request
	resp    *http.Response
	err     error
	calls   int
}

func (c *captureRequester) Do(req *http.Request) (*http.Response, error) {
	c.calls++
	c.request = req
	c.method = req.Method
	c.path = req.URL.String()
	if c.resp == nil {
		c.resp = &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: req}
	}
	return c.resp, c.err
}

func TestDoUploadLocalPathAndFileScheme(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tmp := t.TempDir()
	body := "hello upload"
	req := &captureRequester{}

	rawPath := filepath.Join(tmp, "plain", "payload.txt")
	if err := os.MkdirAll(filepath.Dir(rawPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rawPath, []byte("old payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DoUpload(ctx, req, rawPath, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("DoUpload raw path returned error: %v", err)
	}
	got, err := os.ReadFile(rawPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if string(got) != body {
		t.Fatalf("unexpected raw path payload %q", got)
	}

	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(tmp, "url", "payload.txt"))}).String()
	if _, err := DoUpload(ctx, req, fileURL, strings.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("DoUpload file URL returned error: %v", err)
	}
	urlPath := filepath.Join(tmp, "url", "payload.txt")
	got, err = os.ReadFile(urlPath)
	if err != nil {
		t.Fatalf("ReadFile file URL returned error: %v", err)
	}
	if string(got) != body {
		t.Fatalf("unexpected file URL payload %q", got)
	}

	emptyPath := filepath.Join(tmp, "empty", "payload.txt")
	if _, err := DoUpload(ctx, req, emptyPath, strings.NewReader(""), 0); err != nil {
		t.Fatalf("DoUpload returned error for an empty local upload: %v", err)
	}
	got, err = os.ReadFile(emptyPath)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty local upload data=%q err=%v", got, err)
	}

	if _, err := DoUpload(ctx, req, "", strings.NewReader("x"), 1); err == nil || !strings.Contains(err.Error(), "invalid file upload url") {
		t.Fatalf("expected invalid file upload url error, got %v", err)
	}
}

func TestDoUploadLocalCanceledContextPreservesDestination(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	requester := &captureRequester{}
	_, err := DoUpload(ctx, requester, destination, strings.NewReader("replacement"), int64(len("replacement")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DoUpload error = %v, want context cancellation", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil || string(got) != "original" {
		t.Fatalf("destination after canceled upload = %q, read error = %v; want original bytes", got, readErr)
	}
	if requester.calls != 0 {
		t.Fatalf("HTTPDoer called %d times for local upload", requester.calls)
	}
}

func TestDoUploadLocalStopsOnCancellationDuringCopy(t *testing.T) {
	destination := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	body := &cancelAfterFirstRead{cancel: cancel}

	_, err := DoUpload(ctx, &captureRequester{}, destination, body, int64(len("replacement")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DoUpload error = %v, want context cancellation", err)
	}
	got, readErr := os.ReadFile(destination)
	if readErr != nil || string(got) != "original" {
		t.Fatalf("destination after canceled upload = %q, read error = %v; want original bytes", got, readErr)
	}
}

type cancelAfterFirstRead struct {
	cancel context.CancelFunc
	done   bool
}

func (r *cancelAfterFirstRead) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	n := copy(p, "replacement")
	r.cancel()
	return n, nil
}

func TestDoUploadLocalSizeMismatchPreservesDestination(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		size int64
	}{
		{name: "short", body: "short", size: 10},
		{name: "long", body: "longer than declared", size: 5},
		{name: "long when zero was declared", body: "unexpected", size: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "payload")
			if err := os.WriteFile(destination, []byte("original"), 0o600); err != nil {
				t.Fatal(err)
			}

			_, err := DoUpload(context.Background(), &captureRequester{}, destination, strings.NewReader(test.body), test.size)
			if err == nil || !strings.Contains(err.Error(), "size") {
				t.Fatalf("DoUpload error = %v, want a size mismatch", err)
			}
			got, readErr := os.ReadFile(destination)
			if readErr != nil || string(got) != "original" {
				t.Fatalf("destination after mismatched upload = %q, read error = %v; want original bytes", got, readErr)
			}
		})
	}
}

func TestDoUploadLocalNewDestinationUsesDefaultCreationMode(t *testing.T) {
	directory := t.TempDir()
	modeReference := filepath.Join(directory, "mode-reference")
	reference, err := os.OpenFile(modeReference, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if err := reference.Close(); err != nil {
		t.Fatal(err)
	}
	referenceInfo, err := os.Stat(modeReference)
	if err != nil {
		t.Fatal(err)
	}

	destination := filepath.Join(directory, "payload")
	if _, err := DoUpload(context.Background(), &captureRequester{}, destination, strings.NewReader("payload"), int64(len("payload"))); err != nil {
		t.Fatalf("DoUpload returned error: %v", err)
	}
	destinationInfo, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := destinationInfo.Mode().Perm(), referenceInfo.Mode().Perm(); got != want {
		t.Fatalf("new destination mode = %04o, want normal 0644-with-umask mode %04o", got, want)
	}
}

func TestDoUploadHTTPModesAndErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("azure signed put sets headers and trims etag", func(t *testing.T) {
		req := &captureRequester{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{"\"etag-123\""}}, Body: io.NopCloser(strings.NewReader("ok"))}}
		urlStr := "https://acct.blob.core.windows.net/container/blob?sr=b&sig=sig&sv=2024-01-01"
		etag, err := DoUpload(ctx, req, urlStr, strings.NewReader("payload"), 7)
		if err != nil {
			t.Fatalf("DoUpload returned error: %v", err)
		}
		if etag != "etag-123" {
			t.Fatalf("expected trimmed etag, got %q", etag)
		}
		if req.method != http.MethodPut {
			t.Fatalf("expected PUT method, got %s", req.method)
		}
		if req.request.Header.Get("x-ms-blob-type") != "BlockBlob" {
			t.Fatalf("expected azure blob header, got %s", req.request.Header.Get("x-ms-blob-type"))
		}
		if req.request.Header.Get(request.SkipAuthHeader) != "true" {
			t.Fatal("expected skip auth for signed URL")
		}
		if req.request.ContentLength != 7 {
			t.Fatalf("expected content length 7, got %d", req.request.ContentLength)
		}
	})

	t.Run("gcs media upload uses post", func(t *testing.T) {
		req := &captureRequester{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}}
		urlStr := "https://storage.googleapis.com/upload/storage/v1/b/my-bucket/o?uploadType=media&name=obj.txt&X-Goog-Signature=sig"
		if _, err := DoUpload(ctx, req, urlStr, strings.NewReader("payload"), 0); err != nil {
			t.Fatalf("DoUpload returned error: %v", err)
		}
		if req.method != http.MethodPost {
			t.Fatalf("expected POST for gcs media upload, got %s", req.method)
		}
		if _, ok := req.request.Header["X-Ms-Blob-Type"]; ok {
			t.Fatalf("did not expect azure header in gcs mode, got %+v", req.request.Header)
		}
	})

	t.Run("ordinary expiry upload keeps auth enabled", func(t *testing.T) {
		req := &captureRequester{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}}
		if _, err := DoUpload(ctx, req, "https://api.example/upload?Expires=1", strings.NewReader("payload"), 7); err != nil {
			t.Fatalf("DoUpload returned error: %v", err)
		}
		if req.request.Header.Get(request.SkipAuthHeader) == "true" {
			t.Fatal("did not expect skip auth for ordinary expiry query")
		}
	})

	t.Run("request and status errors bubble up", func(t *testing.T) {
		reqErr := &captureRequester{err: errors.New("boom")}
		if _, err := DoUpload(ctx, reqErr, "https://example.test/upload", strings.NewReader("x"), 1); err == nil || !strings.Contains(err.Error(), "upload to https://example.test/upload failed") {
			t.Fatalf("expected wrapped requester error, got %v", err)
		}

		statusErr := &captureRequester{resp: &http.Response{StatusCode: http.StatusForbidden, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("denied"))}}
		if _, err := DoUpload(ctx, statusErr, "https://example.test/upload", strings.NewReader("x"), 1); err == nil || !strings.Contains(err.Error(), "status 403 body=denied") {
			t.Fatalf("expected status/body error, got %v", err)
		}
	})

	t.Run("non-2xx redirects returned by doer are errors and signed query is redacted", func(t *testing.T) {
		const signedURL = "https://upload.example/file?X-Goog-Signature=upload-secret"
		for _, status := range []int{http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
			t.Run(http.StatusText(status), func(t *testing.T) {
				redirect := &captureRequester{resp: &http.Response{
					StatusCode: status,
					Header:     http.Header{"Location": []string{"https://upload.example/elsewhere"}},
					Body:       io.NopCloser(strings.NewReader("redirected")),
				}}

				_, err := DoUpload(ctx, redirect, signedURL, strings.NewReader("x"), 1)
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("status %d", status)) {
					t.Fatalf("DoUpload error = %v, want status %d error", err, status)
				}
				if strings.Contains(err.Error(), "upload-secret") || strings.Contains(err.Error(), "X-Goog-Signature") {
					t.Fatalf("DoUpload error leaked signed URL query: %v", err)
				}
				if redirect.calls != 1 {
					t.Fatalf("HTTPDoer calls = %d, want the returned redirect response to be rejected without another request", redirect.calls)
				}
			})
		}
	})

	t.Run("request errors redact signed query and preserve cause", func(t *testing.T) {
		const signedURL = "https://upload.example/file?X-Goog-Signature=upload-secret"
		cause := &url.Error{Op: http.MethodPut, URL: signedURL, Err: errors.New("connection refused")}
		requester := &captureRequester{err: cause}

		_, err := DoUpload(ctx, requester, signedURL, strings.NewReader("x"), 1)
		if err == nil {
			t.Fatal("DoUpload returned nil error for requester failure")
		}
		if strings.Contains(err.Error(), "upload-secret") || strings.Contains(err.Error(), "X-Goog-Signature") {
			t.Fatalf("DoUpload error leaked signed URL query: %v", err)
		}
		var gotCause *url.Error
		if !errors.As(err, &gotCause) || gotCause.Err.Error() != "connection refused" {
			t.Fatalf("DoUpload error lost requester cause: %v", err)
		}
	})
}

func TestGenericDownloadOptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	start := int64(3)
	end := int64(9)
	resp := &http.Response{StatusCode: http.StatusPartialContent, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}
	req := &captureRequester{resp: resp}
	urlStr := "https://download.example/file?AWSAccessKeyId=test-key&Expires=1700000000"

	got, err := GenericDownload(ctx, req, urlStr, &start, &end)
	if err != nil {
		t.Fatalf("GenericDownload returned error: %v", err)
	}
	if got != resp {
		t.Fatal("expected returned response pointer")
	}
	if req.method != http.MethodGet {
		t.Fatalf("expected GET method, got %s", req.method)
	}
	if req.request.Header.Get("Range") != "bytes=3-9" {
		t.Fatalf("unexpected Range header: %s", req.request.Header.Get("Range"))
	}
	if req.request.Header.Get(request.SkipAuthHeader) != "true" {
		t.Fatal("expected skip auth for signed URL")
	}

	noEnd := &captureRequester{resp: resp}
	if _, err := GenericDownload(ctx, noEnd, "https://download.example/file", &start, nil); err != nil {
		t.Fatalf("GenericDownload without end returned error: %v", err)
	}
	if noEnd.request.Header.Get("Range") != "bytes=3-" {
		t.Fatalf("unexpected open-ended range header: %s", noEnd.request.Header.Get("Range"))
	}
	if noEnd.request.Header.Get(request.SkipAuthHeader) == "true" {
		t.Fatal("did not expect skip auth for non-presigned URL")
	}

	azure := &captureRequester{resp: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}}
	azureURL := "https://acct.blob.core.windows.net/container/blob?sv=2024-01-01&sig=abc%2Fdef&se=2030-01-01"
	if _, err := GenericDownload(ctx, azure, azureURL, nil, nil); err != nil {
		t.Fatalf("GenericDownload Azure SAS returned error: %v", err)
	}
	if azure.request.Header.Get(request.SkipAuthHeader) != "true" {
		t.Fatal("expected skip auth for Azure SAS")
	}
	if azure.path != azureURL {
		t.Fatalf("request URL changed from signed URL: got %q want %q", azure.path, azureURL)
	}

	ordinary := &captureRequester{resp: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok"))}}
	if _, err := GenericDownload(ctx, ordinary, "https://api.example/file?Expires=1", nil, nil); err != nil {
		t.Fatalf("GenericDownload ordinary expiry URL returned error: %v", err)
	}
	if ordinary.request.Header.Get(request.SkipAuthHeader) == "true" {
		t.Fatal("did not expect skip auth for ordinary expiry query")
	}
}

func TestGenericDownloadRedactsTransportErrors(t *testing.T) {
	const signedURL = "https://download-user:download-password@download.example/file?X-Amz-Signature=synthetic-download-secret#synthetic-fragment"
	sentinel := errors.New("connection refused")
	transportErr := fmt.Errorf("wrapped transport failure: %w", &url.Error{
		Op:  http.MethodGet,
		URL: signedURL,
		Err: sentinel,
	})
	requester := &captureRequester{err: transportErr}

	_, err := GenericDownload(context.Background(), requester, signedURL, nil, nil)
	if err == nil {
		t.Fatal("GenericDownload returned nil error for transport failure")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("GenericDownload error lost transport cause: %v", err)
	}
	for _, secret := range []string{"download-user", "download-password", "X-Amz-Signature", "synthetic-download-secret", "synthetic-fragment"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("GenericDownload error leaked %q: %v", secret, err)
		}
	}
	if !strings.Contains(err.Error(), "https://download.example/file") {
		t.Fatalf("GenericDownload error omitted useful URL context: %v", err)
	}
	var requestErr *url.Error
	if !errors.As(err, &requestErr) {
		t.Fatalf("GenericDownload error lost url.Error context: %v", err)
	}
	if requestErr.URL != "https://download.example/file" {
		t.Fatalf("sanitized url.Error URL = %q", requestErr.URL)
	}

	ordinary := errors.New("connection reset")
	requester = &captureRequester{err: ordinary}
	_, err = GenericDownload(context.Background(), requester, "https://download.example/ordinary", nil, nil)
	if err == nil || !strings.Contains(err.Error(), "download from https://download.example/ordinary failed") || !errors.Is(err, ordinary) {
		t.Fatalf("ordinary transport error lost context or cause: %v", err)
	}
}

func TestSectionReadCloserClosesUnderlyingFileOnce(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("0123456789"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	reader := &sectionReadCloser{reader: io.NewSectionReader(file, 3, 4), closer: file}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "3456" {
		t.Fatalf("section bytes = %q, want 3456", got)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if _, err := file.Stat(); err == nil {
		t.Fatal("underlying file remained usable after close")
	}
}

func TestGenericDownloadLocalRangeClosesBody(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	start, end := int64(3), int64(6)
	resp, err := GenericDownload(context.Background(), &captureRequester{}, path, &start, &end)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "3456" || resp.ContentLength != 4 || resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 3-6/10" {
		t.Fatalf("local range response bytes=%q length=%d status=%d Content-Range=%q", got, resp.ContentLength, resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("second body close returned error: %v", err)
	}
}

func TestGenericDownloadLocalRangeAtEOFIsUnsatisfiable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	start, end := int64(4), int64(4)
	resp, err := GenericDownload(context.Background(), &captureRequester{}, path, &start, &end)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable || resp.Header.Get("Content-Range") != "bytes */4" {
		t.Fatalf("unsatisfied local range status=%d Content-Range=%q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
}

func TestGenericDownloadLocalRangeDoesNotLeakDescriptors(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("/dev/fd is not available on Windows")
	}

	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, []byte("0123456789"), 0o600); err != nil {
		t.Fatal(err)
	}
	countDescriptors := func() int {
		dir, err := os.Open("/dev/fd")
		if err != nil {
			t.Fatalf("read descriptor directory: %v", err)
		}
		defer dir.Close()
		entries, err := dir.Readdirnames(-1)
		if err != nil {
			t.Fatalf("read descriptor names: %v", err)
		}
		return len(entries)
	}

	before := countDescriptors()
	for i := 0; i < 128; i++ {
		start, end := int64(2), int64(7)
		resp, err := GenericDownload(context.Background(), &captureRequester{}, path, &start, &end)
		if err != nil {
			t.Fatalf("range download %d: %v", i, err)
		}
		if _, err := io.Copy(io.Discard, resp.Body); err != nil {
			t.Fatalf("read range download %d: %v", i, err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close range download %d: %v", i, err)
		}
	}
	after := countDescriptors()
	if after > before+2 {
		t.Fatalf("local ranged downloads leaked descriptors: before=%d after=%d", before, after)
	}
}

func TestSignedURLHelpers(t *testing.T) {
	t.Parallel()

	azureURL, _ := url.Parse("https://acct.blob.core.windows.net/c/blob?sr=b&sig=s&sv=2024")
	if !needsAzureBlobTypeHeader(azureURL) {
		t.Fatal("expected azure blob type header requirement")
	}
	if needsAzureBlobTypeHeader(nil) {
		t.Fatal("nil URL should not require azure header")
	}
	noSig, _ := url.Parse("https://acct.blob.core.windows.net/c/blob?sr=b&sv=2024")
	if needsAzureBlobTypeHeader(noSig) {
		t.Fatal("missing sig should not require azure header")
	}
	withComp, _ := url.Parse("https://acct.blob.core.windows.net/c/blob?sr=b&sv=2024&sig=s&comp=block")
	if needsAzureBlobTypeHeader(withComp) {
		t.Fatal("comp query should disable azure blob header")
	}

	gcsURL, _ := url.Parse("https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=media&name=object")
	if !useGCSJSONMediaUpload(gcsURL) {
		t.Fatal("expected gcs media upload detection")
	}
	noName, _ := url.Parse("https://storage.googleapis.com/upload/storage/v1/b/bucket/o?uploadType=media")
	if useGCSJSONMediaUpload(noName) {
		t.Fatal("missing name should disable gcs media upload")
	}
	wrongPath, _ := url.Parse("https://storage.googleapis.com/storage/v1/b/bucket/o?uploadType=media&name=object")
	if useGCSJSONMediaUpload(wrongPath) {
		t.Fatal("wrong path should disable gcs media upload")
	}
}
