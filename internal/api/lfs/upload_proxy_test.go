package lfs

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/transfers"
)

func TestLFSUploadProxyNoBucket507(t *testing.T) {
	ResetLFSLimitersForTest()
	router, db := newLFSRouter()
	db.Credentials = map[string]buckets.Credential{}
	db.NoDefaultCreds = true
	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", bytes.NewReader([]byte("x")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("expected 507, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSUploadProxySuccess(t *testing.T) {
	// 1. Setup mock upload server
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", "mock-etag")
		w.WriteHeader(http.StatusOK)
	}))
	defer uploadServer.Close()

	// 2. Setup Syfon router with mocked dependencies
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{},
	}
	storageFake := &lfsStorageFake{uploadURL: uploadServer.URL}
	app := fiber.New()
	deps := newLFSDependencies(db)
	deps.Storage = core.StoragePorts{Access: storageFake, Multipart: storageFake}
	om := core.NewObjectManager(deps)
	RegisterLFSRoutes(app, om, DefaultOptions())
	router := &fiberTestRouter{app: app}

	// 3. Perform upload proxy request
	oid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader([]byte("small content")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	// 4. Verify results
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if storageFake.initCalled != 1 {
		t.Errorf("expected 1 InitMultipartUpload call, got %d", storageFake.initCalled)
	}
	if storageFake.completeCalled != 1 {
		t.Errorf("expected 1 CompleteMultipartUpload call, got %d", storageFake.completeCalled)
	}
	if storageFake.partUploadID != "mock-upload-id" || storageFake.completeID != "mock-upload-id" {
		t.Fatalf("expected opaque upload ID to flow through multipart calls, got part=%q complete=%q", storageFake.partUploadID, storageFake.completeID)
	}
	if len(storageFake.completeParts) != 1 || storageFake.completeParts[0].PartNumber != 1 || storageFake.completeParts[0].ETag != "mock-etag" {
		t.Fatalf("unexpected completed parts: %+v", storageFake.completeParts)
	}
}

func TestLFSUploadProxyUsesPendingScopedCanonicalLocation(t *testing.T) {
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", "mock-etag")
		w.WriteHeader(http.StatusOK)
	}))
	defer uploadServer.Close()

	oid := strings.Repeat("b", 64)
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{},
		Credentials: map[string]buckets.Credential{
			"syfon-e2e-bucket": {Bucket: "syfon-e2e-bucket", Provider: "s3", Region: "us-west-2"},
		},
		BucketScopes: map[string]buckets.Scope{
			"syfon|": {
				Organization: "syfon",
				Bucket:       "syfon-e2e-bucket",
				PathPrefix:   "program-root",
			},
			"syfon|e2e": {
				Organization: "syfon",
				ProjectID:    "e2e",
				Bucket:       "syfon-e2e-bucket",
				PathPrefix:   "project-subpath",
			},
		},
		PendingMeta: map[string]transfers.PendingMetadata{
			oid: {
				OID: oid,
				Candidate: objects.Candidate{
					Name:             ptr("scoped-lfs.bin"),
					Size:             ptr(int64(13)),
					Checksums:        &[]objects.Checksum{{Type: "sha256", Checksum: oid}},
					ControlledAccess: &[]string{"/organization/syfon/project/e2e"},
					AccessMethods: &[]objects.AccessMethod{{
						Type:      "s3",
						AccessUrl: &objects.AccessURL{Url: "s3://objects/stale-object-id"},
					}},
				},
				CreatedAt: time.Now().UTC(),
				ExpiresAt: time.Now().UTC().Add(time.Minute),
			},
		},
	}
	storageFake := &lfsStorageFake{uploadURL: uploadServer.URL}
	app := fiber.New()
	deps := newLFSDependencies(db)
	deps.Storage = core.StoragePorts{Access: storageFake, Multipart: storageFake}
	om := core.NewObjectManager(deps)
	RegisterLFSRoutes(app, om, DefaultOptions())
	router := &fiberTestRouter{app: app}

	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/"+oid, bytes.NewReader([]byte("small content")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if storageFake.initBucket != "syfon-e2e-bucket" {
		t.Fatalf("expected scoped bucket syfon-e2e-bucket, got %q", storageFake.initBucket)
	}
	if want := "program-root/project-subpath/" + oid; storageFake.initKey != want {
		t.Fatalf("expected scoped key %q, got %q", want, storageFake.initKey)
	}
}
