package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
	"github.com/gorilla/mux"
)

func newLFSRouterWithOptions(opts Options) (*mux.Router, *testutils.MockDatabase) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	uM := &testutils.MockUrlManager{}
	router := mux.NewRouter()
	RegisterLFSRoutes(router, db, uM, opts)
	return router, db
}

func newLFSRouter() (*mux.Router, *testutils.MockDatabase) {
	return newLFSRouterWithOptions(DefaultOptions())
}

func TestLFSBatchDownloadFound(t *testing.T) {
	router, db := newLFSRouter()
	oid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	db.Objects[oid] = &drs.DrsObject{
		Id: oid,
		AccessMethods: []drs.AccessMethod{
			{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/" + oid}},
		},
	}

	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": oid, "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp lfsapi.BatchResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(resp.Objects) != 1 || resp.Objects[0].Actions == nil || resp.Objects[0].Actions.Download == nil || resp.Objects[0].Actions.Download.Href == "" {
		t.Fatalf("expected download action, got %+v", resp)
	}
}

func TestLFSBatchUploadReturnsActionsWithoutPlaceholder(t *testing.T) {
	router, db := newLFSRouter()
	oid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	body := map[string]any{
		"operation": "upload",
		"objects":   []map[string]any{{"oid": oid, "size": 123}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if _, ok := db.Objects[oid]; ok {
		t.Fatalf("did not expect object to be created during batch upload")
	}
	var resp lfsapi.BatchResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	if len(resp.Objects) != 1 || resp.Objects[0].Actions == nil || resp.Objects[0].Actions.Upload == nil || resp.Objects[0].Actions.Upload.Href == "" {
		t.Fatalf("expected upload action in response, got %+v", resp)
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
		AccessMethods: []drs.AccessMethod{
			{
				Type:      "s3",
				AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://test-bucket-1/cbds/end_to_end_test/" + oid},
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

func TestLFSMetadataThenVerifyRegistersObject(t *testing.T) {
	router, db := newLFSRouter()
	oid := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"

	meta := map[string]any{
		"candidates": []map[string]any{
			{
				"name": "forge-file",
				"size": 123,
				"checksums": []map[string]any{
					{"type": "sha256", "checksum": oid},
				},
				"access_methods": []map[string]any{
					{
						"type": "s3",
						"access_url": map[string]any{
							"url": "s3://test-bucket-1/path/" + oid,
						},
					},
				},
			},
		},
	}
	metaRaw, _ := json.Marshal(meta)
	metaReq := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/metadata", bytes.NewReader(metaRaw))
	metaRR := httptest.NewRecorder()
	router.ServeHTTP(metaRR, metaReq)
	if metaRR.Code != http.StatusOK {
		t.Fatalf("expected 200 from metadata, got %d body=%s", metaRR.Code, metaRR.Body.String())
	}

	verifyBody := map[string]any{"oid": oid, "size": 123}
	verifyRaw, _ := json.Marshal(verifyBody)
	verifyReq := httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(verifyRaw))
	verifyReq.Header.Set("Accept", "application/vnd.git-lfs+json")
	verifyReq.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	verifyRR := httptest.NewRecorder()
	router.ServeHTTP(verifyRR, verifyReq)
	if verifyRR.Code != http.StatusOK {
		t.Fatalf("expected 200 from verify, got %d body=%s", verifyRR.Code, verifyRR.Body.String())
	}
	matches, err := db.GetObjectsByChecksum(context.Background(), oid)
	if err != nil {
		t.Fatalf("expected checksum lookup to succeed, got error: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("expected object with checksum %s to be registered on verify", oid)
	}
}

func TestLFSVerifyNotFound(t *testing.T) {
	router, _ := newLFSRouter()
	oid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	body := map[string]any{"oid": oid, "size": 10}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSBatchRejectsBadAccept(t *testing.T) {
	router, _ := newLFSRouter()
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotAcceptable {
		t.Fatalf("expected 406, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSBatchRejectsBadContentType(t *testing.T) {
	router, _ := newLFSRouter()
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSBatchGen3MissingAuthReturns401(t *testing.T) {
	router, _ := newLFSRouter()
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("LFS-Authenticate") == "" {
		t.Fatalf("expected LFS-Authenticate header on 401")
	}
}

func TestLFSBatchTooManyObjects413(t *testing.T) {
	resetLFSLimitersForTest()
	opts := DefaultOptions()
	opts.MaxBatchObjects = 1
	router, _ := newLFSRouterWithOptions(opts)
	body := map[string]any{
		"operation": "download",
		"objects": []map[string]any{
			{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10},
			{"oid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "size": 20},
		},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSBatchRateLimit429(t *testing.T) {
	resetLFSLimitersForTest()
	opts := DefaultOptions()
	opts.RequestLimitPerMinute = 1
	router, _ := newLFSRouterWithOptions(opts)
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req1 := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req1.RemoteAddr = "test-client:1"
	req1.Header.Set("Accept", "application/vnd.git-lfs+json")
	req1.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr1 := httptest.NewRecorder()
	router.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected first request 200, got %d body=%s", rr1.Code, rr1.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req2.RemoteAddr = "test-client:1"
	req2.Header.Set("Accept", "application/vnd.git-lfs+json")
	req2.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d body=%s", rr2.Code, rr2.Body.String())
	}
}

func TestLFSBatchBandwidthLimit509(t *testing.T) {
	resetLFSLimitersForTest()
	opts := DefaultOptions()
	opts.BandwidthLimitBytesPerMinute = 5
	router, _ := newLFSRouterWithOptions(opts)
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.RemoteAddr = "test-client:2"
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != 509 {
		t.Fatalf("expected 509, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSBatchPayloadLimit413(t *testing.T) {
	resetLFSLimitersForTest()
	opts := DefaultOptions()
	opts.MaxBatchBodyBytes = 20
	router, _ := newLFSRouterWithOptions(opts)
	body := map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "size": 10}},
	}
	raw, _ := json.Marshal(body)
	if len(raw) <= 20 {
		t.Fatalf("test setup invalid: payload unexpectedly <= 20 bytes")
	}
	req := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(raw))
	req.ContentLength = int64(len(raw))
	req.Header.Set("Accept", "application/vnd.git-lfs+json")
	req.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestLFSUploadProxyNoBucket507(t *testing.T) {
	resetLFSLimitersForTest()
	router, db := newLFSRouter()
	db.Credentials = map[string]core.S3Credential{}
	db.NoDefaultCreds = true
	req := httptest.NewRequest(http.MethodPut, "/info/lfs/objects/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", bytes.NewReader([]byte("x")))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("expected 507, got %d body=%s", rr.Code, rr.Body.String())
	}
}
