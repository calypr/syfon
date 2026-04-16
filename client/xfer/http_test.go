package xfer

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/pkg/request"
)

type testRequestor struct {
	last *request.RequestBuilder
	resp *http.Response
	err  error
}

func (t *testRequestor) Do(_ context.Context, method, path string, body, out any, opts ...request.RequestOption) error {
	rb := &request.RequestBuilder{Method: method, Url: path, Headers: map[string]string{}}
	if body != nil {
		if reader, ok := body.(io.Reader); ok {
			rb.WithBody(reader)
		}
	}
	for _, opt := range opts {
		opt(rb)
	}
	t.last = rb
	if t.err != nil {
		return t.err
	}
	resp := t.resp
	if resp == nil {
		resp = &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}
	}
	if out == nil {
		return nil
	}
	if outResp, ok := out.(*http.Response); ok {
		*outResp = *resp
		return nil
	}
	if outResp, ok := out.(**http.Response); ok {
		*outResp = resp
		return nil
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func TestNeedsAzureBlobTypeHeader(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "nil url", raw: "", want: false},
		{name: "azure blob sas put candidate", raw: "http://localhost:10000/devstoreaccount1/container/obj?sig=abc&sv=2021-08-06&sr=b", want: true},
		{name: "not blob resource", raw: "http://localhost:10000/devstoreaccount1/container/obj?sig=abc&sv=2021-08-06&sr=c", want: false},
		{name: "has comp means non-put-blob path", raw: "http://localhost:10000/devstoreaccount1/container/obj?sig=abc&sv=2021-08-06&sr=b&comp=block", want: false},
		{name: "missing signature", raw: "http://localhost:10000/devstoreaccount1/container/obj?sv=2021-08-06&sr=b", want: false},
		{name: "non-http scheme", raw: "s3://bucket/key", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var parsed *url.URL
			if tc.raw != "" {
				u, err := url.Parse(tc.raw)
				if err != nil {
					t.Fatalf("parse url: %v", err)
				}
				parsed = u
			}
			if got := needsAzureBlobTypeHeader(parsed); got != tc.want {
				t.Fatalf("needsAzureBlobTypeHeader(%q)=%v want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestUseGCSJSONMediaUpload(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "valid gcs media upload", raw: "http://localhost:4443/upload/storage/v1/b/test-bucket/o?uploadType=media&name=path%2Ffile.txt", want: true},
		{name: "missing name", raw: "http://localhost:4443/upload/storage/v1/b/test-bucket/o?uploadType=media", want: false},
		{name: "wrong upload type", raw: "http://localhost:4443/upload/storage/v1/b/test-bucket/o?uploadType=resumable&name=path%2Ffile.txt", want: false},
		{name: "wrong path", raw: "http://localhost:4443/storage/v1/b/test-bucket/o?uploadType=media&name=path%2Ffile.txt", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.raw)
			if err != nil {
				t.Fatalf("parse url: %v", err)
			}
			if got := useGCSJSONMediaUpload(u); got != tc.want {
				t.Fatalf("useGCSJSONMediaUpload(%q)=%v want %v", tc.raw, got, tc.want)
			}
		})
	}
}

func TestDoUpload_FileScheme(t *testing.T) {
	tmp := t.TempDir()
	dst := filepath.Join(tmp, "uploaded.txt")
	urlStr := "file://" + dst

	req := &testRequestor{}
	if _, err := DoUpload(context.Background(), req, urlStr, strings.NewReader("hello"), 5); err != nil {
		t.Fatalf("DoUpload returned error: %v", err)
	}

	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read uploaded file: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("unexpected file contents: %q", string(got))
	}
}

func TestDoUpload_SelectsMethodAndHeaders(t *testing.T) {
	t.Run("gcs media upload uses POST", func(t *testing.T) {
		req := &testRequestor{}
		_, err := DoUpload(context.Background(), req, "http://localhost:4443/upload/storage/v1/b/test-bucket/o?uploadType=media&name=a.txt", strings.NewReader("x"), 1)
		if err != nil {
			t.Fatalf("DoUpload returned error: %v", err)
		}
		if req.last == nil || req.last.Method != http.MethodPost {
			t.Fatalf("expected POST method, got %#v", req.last)
		}
	})

	t.Run("azure signed put sets blob type header", func(t *testing.T) {
		req := &testRequestor{}
		_, err := DoUpload(context.Background(), req, "http://localhost:10000/devstoreaccount1/c/o.txt?sig=abc&sv=2021-08-06&sr=b", strings.NewReader("x"), 1)
		if err != nil {
			t.Fatalf("DoUpload returned error: %v", err)
		}
		if req.last == nil || req.last.Method != http.MethodPut {
			t.Fatalf("expected PUT method, got %#v", req.last)
		}
		if got := req.last.Headers["x-ms-blob-type"]; got != "BlockBlob" {
			t.Fatalf("expected x-ms-blob-type header, got %q", got)
		}
	})
}

func TestDoUpload_HTTPErrorIncludesBody(t *testing.T) {
	req := &testRequestor{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Body:       io.NopCloser(strings.NewReader("boom")),
		Header:     make(http.Header),
	}}
	_, err := DoUpload(context.Background(), req, "http://example.com/upload", strings.NewReader("x"), 1)
	if err == nil {
		t.Fatal("expected error for non-2xx upload response")
	}
	if !strings.Contains(err.Error(), "status 400") || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGenericDownload_SetsRangeAndSkipAuth(t *testing.T) {
	req := &testRequestor{resp: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("ok")), Header: make(http.Header)}}
	start, end := int64(1), int64(9)
	resp, err := GenericDownload(context.Background(), req, "https://storage.googleapis.com/bucket/key?X-Goog-Signature=abc", &start, &end)
	if err != nil {
		t.Fatalf("GenericDownload returned error: %v", err)
	}
	_ = resp.Body.Close()

	if req.last == nil {
		t.Fatal("expected request builder capture")
	}
	if got := req.last.Headers["Range"]; got != "bytes=1-9" {
		t.Fatalf("unexpected range header: %q", got)
	}
	if !req.last.SkipAuth {
		t.Fatal("expected SkipAuth=true for cloud presigned URL")
	}
}
