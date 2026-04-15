package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
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
	return c
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
		var req UploadBlankRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if req.Guid == nil || *req.Guid != "abc" {
			t.Fatalf("unexpected guid: %v", req.Guid)
		}
		out := UploadBlankResponse{
			Guid:   ptr("abc"),
			Url:    ptr("https://signed"),
			Bucket: ptr("b1"),
		}
		data, _ := json.Marshal(out)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     make(http.Header),
		}, nil
	})
	req := UploadBlankRequest{Guid: ptr("abc")}
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
	rec := InternalRecordRequest{
		Did: "id-1",
	}
	c := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/index" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("hash"); got != "sha256:deadbeef" {
			t.Fatalf("unexpected hash query: %q", got)
		}
		data, _ := json.Marshal(ListRecordsResponse{Records: &[]InternalRecordRequest{rec}})
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     make(http.Header),
		}, nil
	})
	out, err := c.index.List(context.Background(), ListRecordsOptions{Hash: "sha256:deadbeef"})
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
		return &http.Response{
			StatusCode: http.StatusCreated,
			Body:       io.NopCloser(strings.NewReader(`{"guid":"g1","uploadId":"u1"}`)),
			Header:     make(http.Header),
		}, nil
	})
	req := MultipartInitRequest{Guid: ptr("g1")}
	out, err := c.data.MultipartInit(context.Background(), req)
	if err != nil {
		t.Fatalf("MultipartInit failed: %v", err)
	}
	if out.Guid == nil || *out.Guid != "g1" || out.UploadId == nil || *out.UploadId != "u1" {
		t.Fatalf("unexpected response: %+v", out)
	}
}
