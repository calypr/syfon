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
	return New("http://example.test", WithHTTPClient(httpClient))
}

func TestClientBasicAuthAndUserAgent(t *testing.T) {
	t.Parallel()
	c := New("http://example.test",
		WithBasicAuth("u", "p"),
		WithUserAgent("syfon-test-client"),
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
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
		})}))
	if err := c.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

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
		if req.GetGuid() != "abc" {
			t.Fatalf("unexpected guid: %q", req.GetGuid())
		}
		out := UploadBlankResponse{}
		out.SetGuid("abc")
		out.SetUrl("https://signed")
		out.SetBucket("b1")
		data, _ := json.Marshal(out)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     make(http.Header),
		}, nil
	})
	req := UploadBlankRequest{}
	req.SetGuid("abc")
	out, err := c.Data().UploadBlank(context.Background(), req)
	if err != nil {
		t.Fatalf("UploadBlank failed: %v", err)
	}
	if out.GetUrl() != "https://signed" || out.GetBucket() != "b1" {
		t.Fatalf("unexpected response: %+v", out)
	}
}

func TestIndexListByHash(t *testing.T) {
	t.Parallel()
	rec := InternalRecord{}
	rec.SetDid("id-1")
	c := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/index" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.URL.Query().Get("hash"); got != "sha256:deadbeef" {
			t.Fatalf("unexpected hash query: %q", got)
		}
		data, _ := json.Marshal(ListRecordsResponse{Records: []InternalRecord{rec}})
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(string(data))),
			Header:     make(http.Header),
		}, nil
	})
	out, err := c.Index().List(context.Background(), ListRecordsOptions{Hash: "sha256:deadbeef"})
	if err != nil {
		t.Fatalf("Index.List failed: %v", err)
	}
	if len(out.Records) != 1 || out.Records[0].GetDid() != "id-1" {
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
	req := MultipartInitRequest{}
	req.SetGuid("g1")
	out, err := c.Data().MultipartInit(context.Background(), req)
	if err != nil {
		t.Fatalf("MultipartInit failed: %v", err)
	}
	if out.GetGuid() != "g1" || out.GetUploadId() != "u1" {
		t.Fatalf("unexpected response: %+v", out)
	}
}
