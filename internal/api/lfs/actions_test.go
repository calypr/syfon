package lfs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/urlmanager"
)

type captureSigningURLManager struct {
	testutils.MockUrlManager
	signID  string
	signURL string
}

func (m *captureSigningURLManager) SignURL(ctx context.Context, accessID string, url string, opts urlmanager.SignOptions) (string, error) {
	m.signID = accessID
	m.signURL = url
	return m.MockUrlManager.SignURL(ctx, accessID, url, opts)
}

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

func TestPrepareDownloadActions_RewritesScopedObjectURL(t *testing.T) {
	oid := "download-scoped"
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			oid: {
				Id:               oid,
				ControlledAccess: &[]string{"/organization/HTAN_INT/project/BForePC"},
				AccessMethods: &[]drs.AccessMethod{{
					Type: drs.AccessMethodTypeS3,
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://bforepc-prod/OHSU/slide.ome.tiff"},
				}},
			},
		},
		BucketScopes: map[string]models.BucketScope{
			"HTAN_INT|BForePC": {
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Bucket:       "bforepc",
				PathPrefix:   "bforepc-prod",
			},
		},
	}
	um := &captureSigningURLManager{}
	om := core.NewObjectManager(db, um)

	actions, objErr := prepareDownloadActions(context.Background(), om, oid)
	if objErr != nil {
		t.Fatalf("expected download action, got error: %+v", objErr)
	}
	if actions == nil || actions.Download == nil || actions.Download.Href == "" {
		t.Fatalf("expected signed download action, got %+v", actions)
	}
	wantURL := "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff"
	if um.signURL != wantURL {
		t.Fatalf("expected scoped LFS download URL %q, got %q", wantURL, um.signURL)
	}
	if um.signID != "bforepc" {
		t.Fatalf("expected signer credential bucket bforepc, got %q", um.signID)
	}
}
