package lfs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/usage"
)

func TestLFSBatchDownloadUsesTransferAndUsagePorts(t *testing.T) {
	oid := strings.Repeat("a", 64)
	db := &testutils.MockDatabase{
		Objects: map[string]*objects.Record{
			oid: {
				Id:        objects.RecordID(oid),
				Size:      10,
				Checksums: []objects.Checksum{{Type: "sha256", Checksum: oid}},
				AccessMethods: &[]objects.AccessMethod{{
					Type:      "s3",
					AccessUrl: &objects.AccessURL{Url: "s3://bucket/" + oid},
				}},
			},
		},
		Credentials: map[string]buckets.Credential{"bucket": {Bucket: "bucket"}},
	}
	storageFake := &lfsTestStorage{}
	router := newLFSTestRouter(db, storageFake, DefaultOptions())
	body, _ := json.Marshal(map[string]any{
		"operation": "download",
		"objects":   []map[string]any{{"oid": oid, "size": 10}},
	})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/batch", bytes.NewReader(body))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("batch status = %d body=%s", response.Code, response.Body.String())
	}
	var payload lfsapi.BatchResponse
	if err := json.Unmarshal(response.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if len(payload.Objects) != 1 || payload.Objects[0].Actions == nil || payload.Objects[0].Actions.Download == nil {
		t.Fatalf("download actions = %+v", payload.Objects)
	}
	if len(db.TransferEvents) != 1 || db.TransferEvents[0].EventType != usage.TransferEventAccessIssued {
		t.Fatalf("transfer events = %+v", db.TransferEvents)
	}
}

func TestLFSMetadataVerifyPreservesPendingPopBeforeRegister(t *testing.T) {
	oid := strings.Repeat("b", 64)
	db := &testutils.MockDatabase{Objects: map[string]*objects.Record{}, Credentials: map[string]buckets.Credential{"bucket": {Bucket: "bucket"}}}
	router := newLFSTestRouter(db, &lfsTestStorage{}, DefaultOptions())
	metadata, _ := json.Marshal(map[string]any{"candidates": []map[string]any{{
		"name": "object.bin", "size": 12,
		"checksums":      []map[string]any{{"type": "sha256", "checksum": oid}},
		"access_methods": []map[string]any{{"type": "s3", "access_url": map[string]any{"url": "s3://bucket/" + oid}}},
	}}})
	request := httptest.NewRequest(http.MethodPost, "/info/lfs/objects/metadata", bytes.NewReader(metadata))
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metadata status = %d body=%s", response.Code, response.Body.String())
	}
	verify, _ := json.Marshal(map[string]any{"oid": oid, "size": 12})
	request = httptest.NewRequest(http.MethodPost, "/info/lfs/verify", bytes.NewReader(verify))
	request.Header.Set("Accept", "application/vnd.git-lfs+json")
	request.Header.Set("Content-Type", "application/vnd.git-lfs+json")
	response = httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verify status = %d body=%s", response.Code, response.Body.String())
	}
	if _, ok := db.PendingMeta[oid]; ok {
		t.Fatal("pending metadata was not consumed")
	}
}

func TestLFSUploadProxyPreservesOpaqueMultipartAndPartOrder(t *testing.T) {
	oid := strings.Repeat("c", 64)
	db := &testutils.MockDatabase{
		Objects:     map[string]*objects.Record{},
		Credentials: map[string]buckets.Credential{"bucket": {Bucket: "bucket"}},
	}
	storageFake := &lfsTestStorage{}
	server := NewLFSServer(newLFSTestDependencies(db, storageFake), DefaultOptions())
	server.partUploader = func(_ context.Context, _ string, content []byte) (string, error) {
		if string(content) != "payload" {
			t.Fatalf("multipart content = %q", content)
		}
		return "etag", nil
	}
	response, err := server.LfsUploadProxy(context.Background(), lfsapi.LfsUploadProxyRequestObject{
		Oid:  oid,
		Body: bytes.NewReader([]byte("payload")),
	})
	if err != nil {
		t.Fatalf("upload proxy error: %v", err)
	}
	if _, ok := response.(lfsapi.LfsUploadProxy200Response); !ok {
		t.Fatalf("upload proxy response = %T, want 200", response)
	}
	if storageFake.initTarget != (storage.ObjectTarget{Bucket: "bucket", Key: oid}) {
		t.Fatalf("multipart init target = %+v", storageFake.initTarget)
	}
	if storageFake.partRequest.UploadID != "opaque-upload-id" || storageFake.partRequest.PartNumber != 1 {
		t.Fatalf("multipart part request = %+v", storageFake.partRequest)
	}
	if storageFake.complete.UploadID != "opaque-upload-id" || len(storageFake.complete.Parts) != 1 || storageFake.complete.Parts[0].ETag != "etag" {
		t.Fatalf("multipart completion = %+v", storageFake.complete)
	}
}
