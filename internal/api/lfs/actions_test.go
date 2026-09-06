package lfs

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
)

type captureSigningStorage struct {
	lfsStorageFake
	signID  string
	signURL string
	options storage.AccessOptions
}

func (m *captureSigningStorage) Access(_ context.Context, request storage.AccessRequest) (storage.Access, error) {
	m.signID = request.Target.AccessID
	m.signURL = request.Target.Location
	m.options = request.Options
	return storage.Access{Location: request.Target.Location + "?signed=true"}, nil
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
		Objects: map[string]*objects.Record{},
	}
	oid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	did := "did:example:bbbb"
	db.Objects[oid] = &objects.Record{
		Id: objects.RecordID(did),
		AccessMethods: &[]objects.AccessMethod{
			{
				Type:      "s3",
				AccessUrl: &objects.AccessURL{Url: "s3://test-bucket-1/cbds/end_to_end_test/" + oid},
			},
		},
	}

	obj, err := resolveObjectForOID(context.Background(), db, oid)
	if err != nil {
		t.Fatalf("expected checksum fallback object, got error: %v", err)
	}
	if obj == nil || string(obj.Id) != did {
		t.Fatalf("expected object id %s, got %+v", did, obj)
	}
}

func TestPrepareDownloadActions_MapsLegacyReplicaURL(t *testing.T) {
	oid := "download-scoped"
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{
			oid: {
				Id:               objects.RecordID(oid),
				ControlledAccess: &[]string{"/organization/HTAN_INT/project/BForePC"},
				AccessMethods: &[]objects.AccessMethod{{
					Type:      "s3",
					AccessUrl: &objects.AccessURL{Url: "s3://bforepc-prod/OHSU/slide.ome.tiff"},
				}},
			},
		},
		BucketScopes: map[string]buckets.Scope{
			"HTAN_INT|BForePC": {
				Organization: "HTAN_INT",
				ProjectID:    "BForePC",
				Bucket:       "bforepc",
				PathPrefix:   "bforepc-prod",
			},
		},
	}
	storageFake := &captureSigningStorage{}
	deps := newLFSDependencies(db)
	deps.Storage = core.StoragePorts{Access: storageFake}
	objectService := newLFSObjectService(deps)
	om := core.NewObjectManager(deps)

	actions, objErr := prepareDownloadActions(context.Background(), objectService, om, oid)
	if objErr != nil {
		t.Fatalf("expected download action, got error: %+v", objErr)
	}
	if actions == nil || actions.Download == nil || actions.Download.Href == "" {
		t.Fatalf("expected signed download action, got %+v", actions)
	}
	wantURL := "s3://bforepc/bforepc-prod/OHSU/slide.ome.tiff"
	if storageFake.signURL != wantURL {
		t.Fatalf("expected physical LFS replica URL %q, got %q", wantURL, storageFake.signURL)
	}
	if storageFake.signID != "bforepc" {
		t.Fatalf("expected signer credential bucket bforepc, got %q", storageFake.signID)
	}
	if storageFake.options != (storage.AccessOptions{}) {
		t.Fatalf("expected empty download access options, got %+v", storageFake.options)
	}
}

func TestPrepareUploadActionsRequiresGlobalDataFileCreate(t *testing.T) {
	testCases := []struct {
		name       string
		privileges map[string]map[string]bool
		wantCode   int32
	}{
		{
			name:       "allows global create privilege",
			privileges: map[string]map[string]bool{"/data_file": {"create": true}},
		},
		{
			name:       "rejects org-scoped alias privilege",
			privileges: map[string]map[string]bool{"/programs/data_file": {"create": true}},
			wantCode:   int32(http.StatusForbidden),
		},
		{
			name:       "rejects read-only global privilege",
			privileges: map[string]map[string]bool{"/data_file": {"read": true}},
			wantCode:   int32(http.StatusForbidden),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			session := access.NewSession("gen3")
			session.AuthHeaderPresent = true
			session.SetAuthorizations(nil, tc.privileges, true)
			ctx := access.WithSession(context.Background(), session)

			db := &testutils.MockDatabase{Objects: map[string]*objects.Record{}}
			deps := newLFSDependencies(db)
			objectService := newLFSObjectService(deps)
			om := core.NewObjectManager(deps)
			actions, size, objErr := prepareUploadActions(ctx, objectService, om, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 123, "https://example.test")

			if tc.wantCode != 0 {
				if objErr == nil || objErr.Code != tc.wantCode {
					t.Fatalf("expected object error code %d, got %+v", tc.wantCode, objErr)
				}
				if size != 123 {
					t.Fatalf("expected unchanged requested size on denied upload, got %d", size)
				}
				return
			}

			if objErr != nil {
				t.Fatalf("expected success, got object error: %+v", objErr)
			}
			if actions == nil || actions.Upload == nil || actions.Verify == nil {
				t.Fatalf("expected upload and verify actions, got %+v", actions)
			}
			if size != 123 {
				t.Fatalf("expected size 123, got %d", size)
			}
		})
	}
}
