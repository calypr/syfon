package drs

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/pkg/request"
)

type fakeRequest struct {
	lastMethod string
	lastURL    string
}

func (f *fakeRequest) New(method, url string) *request.RequestBuilder {
	f.lastMethod = method
	f.lastURL = url
	return &request.RequestBuilder{
		Method:  method,
		Url:     url,
		Headers: map[string]string{},
	}
}

func (f *fakeRequest) Do(ctx context.Context, req *request.RequestBuilder) (*http.Response, error) {
	_ = ctx
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(`{"records":[]}`)),
	}, nil
}

func TestGetObjectByHash_UsesTypedChecksumQuery(t *testing.T) {
	req := &fakeRequest{}
	c := NewLocalDrsClient(req, "http://example.org", nil)
	_, err := c.GetObjectByHash(context.Background(), &hash.Checksum{
		Type:     string(hash.ChecksumTypeSHA512),
		Checksum: strings.Repeat("a", 128),
	})
	if err != nil {
		t.Fatalf("GetObjectByHash returned error: %v", err)
	}
	if req.lastMethod != http.MethodGet {
		t.Fatalf("expected GET, got %s", req.lastMethod)
	}
	want := "/index?hash=sha512%3A" + strings.Repeat("a", 128)
	if !strings.HasSuffix(req.lastURL, want) {
		t.Fatalf("expected URL suffix %q, got %q", want, req.lastURL)
	}
}

func TestGetObjectByHash_InvalidChecksumFails(t *testing.T) {
	req := &fakeRequest{}
	c := NewLocalDrsClient(req, "http://example.org", nil)
	_, err := c.GetObjectByHash(context.Background(), &hash.Checksum{
		Type:     string(hash.ChecksumTypeSHA512),
		Checksum: "abc",
	})
	if err == nil {
		t.Fatalf("expected validation error for invalid checksum")
	}
}
