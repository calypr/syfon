package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/client/internalapi"
	"github.com/calypr/syfon/client/syfonclient"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newTestClient(t *testing.T, fn roundTripFunc) *Client {
	t.Helper()
	httpClient := &http.Client{Transport: fn}
	c, err := New("http://example.test", WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return c.(*Client)
}

func TestClientBasicAuthAndUserAgent(t *testing.T) {
	t.Parallel()
	httpClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/healthz" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "u" || pass != "p" {
			t.Fatalf("missing/invalid basic auth user=%q pass=%q ok=%v", user, pass, ok)
		}
		if got := r.Header.Get("User-Agent"); got != "syfon-test-client" {
			t.Fatalf("unexpected user agent: %q", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}

	c, err := New("http://example.test",
		WithBasicAuth("u", "p"),
		WithUserAgent("syfon-test-client"),
		WithHTTPClient(httpClient))
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	if err := c.Health().Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func ptr[T any](v T) *T { return &v }

func TestDataUploadBlank(t *testing.T) {
	t.Parallel()
	c := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/upload" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		var req internalapi.InternalUploadBlankRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Guid == nil || *req.Guid != "abc" {
			t.Fatalf("unexpected guid: %v", req.Guid)
		}
		out := internalapi.InternalUploadBlankOutput{
			Guid:   ptr("abc"),
			Url:    ptr("https://signed"),
			Bucket: ptr("b1"),
		}
		data, _ := json.Marshal(out)
		header := make(http.Header)
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     header,
		}, nil
	})
	req := internalapi.InternalUploadBlankRequest{Guid: ptr("abc")}
	out, err := c.data.UploadBlank(context.Background(), req)
	if err != nil {
		t.Fatalf("UploadBlank failed: %v", err)
	}
	if out.Url == nil || *out.Url != "https://signed" || out.Bucket == nil || *out.Bucket != "b1" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestIndexListByHash(t *testing.T) {
	t.Parallel()
	rec := internalapi.InternalRecord{
		Did: "id-1",
	}
	c := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/index" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("hash"); got != "sha256:deadbeef" {
			t.Fatalf("unexpected hash query: %q", got)
		}
		data, _ := json.Marshal(internalapi.ListRecordsResponse{Records: &[]internalapi.InternalRecord{rec}})
		header := make(http.Header)
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     header,
		}, nil
	})
	out, err := c.index.List(context.Background(), syfonclient.ListRecordsOptions{Hash: "sha256:deadbeef"})
	if err != nil {
		t.Fatalf("Index.List failed: %v", err)
	}
	if out.Records == nil || len(*out.Records) != 1 || (*out.Records)[0].Did != "id-1" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestDataMultipartInitUsesCanonicalUploadId(t *testing.T) {
	t.Parallel()
	c := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/data/multipart/init" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		header := make(http.Header)
		header.Set("Content-Type", "application/json")
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(`{"guid":"g1","uploadId":"u1"}`)),
			Header:     header,
		}, nil
	})
	// InitMultipartUpload returns (uploadID string, respGuid string, err error)
	// Wait, I updated the service methods.
	// c.data.InitMultipartUpload(ctx, guid, filename, bucket) -> (string, string, error)
	uploadID, respGuid, err := c.data.InitMultipartUpload(context.Background(), "g1", "", "")
	if err != nil {
		t.Fatalf("MultipartInit failed: %v", err)
	}
	if respGuid != "g1" || uploadID != "u1" {
		t.Fatalf("unexpected response: guid=%s uploadID=%s", respGuid, uploadID)
	}
}
