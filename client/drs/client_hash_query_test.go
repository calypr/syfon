package drs

import (
	"context"
	"encoding/json"
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

func (f *fakeRequest) Do(ctx context.Context, method, path string, in, out any, opts ...request.RequestOption) error {
	f.lastMethod = method
	f.lastURL = path
	if out != nil {
		return json.NewDecoder(strings.NewReader(`{"records":[]}`)).Decode(out)
	}
	return nil
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
