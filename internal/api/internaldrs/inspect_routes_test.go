package internaldrs

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
)

func TestHandleInternalInspectObjectScopedSuccess(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{Organization: "syfon", Project: "e2e", Key: "nested/file.bin", Scheme: "s3"})
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"b1": {Bucket: "b1"},
		},
		BucketScopes: map[string]models.BucketScope{
			"syfon|":    {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
			"syfon|e2e": {Organization: "syfon", ProjectID: "e2e", Bucket: "b1", PathPrefix: "project-root"},
		},
	}
	om := core.NewObjectManager(db, &testutils.MockUrlManager{})
	om.SetS3ObjectInspector(func(ctx context.Context, cred models.S3Credential, bucket string, key string) (*core.StorageObjectMetadata, error) {
		return &core.StorageObjectMetadata{Bucket: bucket, Key: key, Path: "file.bin", SizeBytes: 17, ETag: "etag-1", LastModTime: time.Date(2026, 6, 11, 1, 2, 3, 0, time.UTC)}, nil
	})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/e2e": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalInspectObjectResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ObjectURL != "s3://b1/program-root/project-root/nested/file.bin" {
		t.Fatalf("unexpected object_url: %s", resp.ObjectURL)
	}
	if resp.Bucket != "b1" || resp.Key != "program-root/project-root/nested/file.bin" {
		t.Fatalf("unexpected location: bucket=%s key=%s", resp.Bucket, resp.Key)
	}
	if resp.ETag != "etag-1" || resp.SizeBytes != 17 {
		t.Fatalf("unexpected metadata: %+v", resp)
	}
	if resp.LastModTime != "2026-06-11T01:02:03Z" {
		t.Fatalf("unexpected last_modified: %s", resp.LastModTime)
	}
}

func TestHandleInternalInspectObjectRawSuccess(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "s3://b1/program-root/raw/file.bin"})
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}},
		BucketScopes: map[string]models.BucketScope{
			"syfon|": {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
		},
	}
	om := core.NewObjectManager(db, &testutils.MockUrlManager{})
	om.SetS3ObjectInspector(func(ctx context.Context, cred models.S3Credential, bucket string, key string) (*core.StorageObjectMetadata, error) {
		return &core.StorageObjectMetadata{Bucket: bucket, Key: key, Path: "file.bin", SizeBytes: 99, ETag: "etag-raw"}, nil
	})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "etag-raw") {
		t.Fatalf("expected raw metadata response, got %s", rr.Body.String())
	}
}

func TestHandleInternalInspectObjectMissingScope(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{Organization: "syfon", Project: "missing", Key: "nested/file.bin", Scheme: "s3"})
	om := core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon/project/missing": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalInspectObjectPermissionDenied(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "s3://b1/program-root/raw/file.bin"})
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}},
		BucketScopes: map[string]models.BucketScope{
			"syfon|": {Organization: "syfon", Bucket: "b1", PathPrefix: "program-root"},
		},
	}
	om := core.NewObjectManager(db, &testutils.MockUrlManager{})
	om.SetS3ObjectInspector(func(ctx context.Context, cred models.S3Credential, bucket string, key string) (*core.StorageObjectMetadata, error) {
		return nil, &core.StorageInspectError{Kind: core.StorageInspectPermissionDenied, Message: "provider denied access to s3://b1/program-root/raw/file.bin"}
	})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalInspectObjectMalformedURL(t *testing.T) {
	body, _ := json.Marshal(internalInspectObjectRequest{ObjectURL: "https://example.com/file.bin"})
	om := core.NewObjectManager(&testutils.MockDatabase{}, &testutils.MockUrlManager{})
	req := withTestAuthzContext(httptest.NewRequest(http.MethodPost, "/data/inspect", bytes.NewBuffer(body)), "gen3", map[string]map[string]bool{"/organization/syfon": {"read": true}})
	rr := doInternalDRSTestRequest(req, om)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}
