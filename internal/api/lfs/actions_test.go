package lfs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/testutils"
)

func TestUploadPartToSignedURLFaultInjection(t *testing.T) {
	origClient := http.DefaultClient
	defer func() { http.DefaultClient = origClient }()
	http.DefaultClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString("ok")),
				Header:     http.Header{},
				Request:    req,
			}, nil
		}),
	}

	if _, err := uploadPartToSignedURL(context.Background(), "http://example.org/upload", []byte("payload")); err == nil {
		t.Fatal("expected multipart upload part to fail when no etag is returned")
	}
}

func TestResolveObjectForOIDFallsBackToChecksum(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	oid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	did := "did:example:bbbb"
	db.Objects[oid] = &drs.DrsObject{
		Id: did,
		AccessMethods: &[]drs.AccessMethod{
			{
				Type: drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://test-bucket-1/cbds/end_to_end_test/" + oid},
			},
		},
	}

	obj, err := resolveObjectForOID(context.Background(), db, oid)
	if err != nil {
		t.Fatalf("expected checksum fallback object, got error: %v", err)
	}
	if obj == nil || obj.Id != did {
		t.Fatalf("expected object id %s, got %+v", did, obj)
	}
}
