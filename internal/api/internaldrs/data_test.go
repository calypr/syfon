package internaldrs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/drs-server/apigen/bucketapi"
	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/internalapi"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func TestHandleInternalDownload(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-file-id": {
				Id: "test-file-id",
				AccessMethods: []drs.AccessMethod{
					{
						Type: "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/key",
						},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req, err := http.NewRequest("GET", "/internal/data/download/test-file-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = mux.SetURLVars(req, map[string]string{"file_id": "test-file-id"})

	rr := httptest.NewRecorder()
	handleInternalDownload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(resp.GetUrl(), "signed=true") {
		t.Errorf("expected signed url, got %v", resp.GetUrl())
	}
}

func TestHandleInternalDownload_ResolvesByChecksum(t *testing.T) {
	const (
		did = "did-123"
		oid = "sha256-abc"
	)
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			did: {
				Id: did,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: oid},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type: "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/cbds/end_to_end_test/" + did + "/" + oid,
						},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req, err := http.NewRequest("GET", "/internal/data/download/"+oid, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = mux.SetURLVars(req, map[string]string{"file_id": oid})

	rr := httptest.NewRecorder()
	handleInternalDownload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.GetUrl(), "/"+did+"/"+oid) {
		t.Fatalf("expected signed url to include DID-backed key, got %s", resp.GetUrl())
	}
}

func TestHandleInternalDownload_ResolvesByUUID(t *testing.T) {
	const (
		did = "2eb7a53c-1309-4be6-b6aa-8ed9249e23a9"
		oid = "sha256-def"
	)
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			did: {
				Id: did,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: oid},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type: "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/cbds/end_to_end_test/" + did,
						},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req, err := http.NewRequest("GET", "/internal/data/download/"+did, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = mux.SetURLVars(req, map[string]string{"file_id": did})

	rr := httptest.NewRecorder()
	handleInternalDownload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.GetUrl(), "/"+did) {
		t.Fatalf("expected signed url to include UUID-backed key, got %s", resp.GetUrl())
	}
}

func TestHandleInternalUploadBlank(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	mockUM := &testutils.MockUrlManager{}

	guid := "new-guid"
	reqBody := internalapi.InternalUploadBlankRequest{Guid: &guid}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/upload", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalUploadBlank(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	var resp internalapi.InternalUploadBlankResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if _, err := uuid.Parse(resp.GetGuid()); err != nil {
		t.Fatalf("expected minted UUID guid, got %q", resp.GetGuid())
	}
	if !strings.Contains(resp.GetUrl(), "upload=true") {
		t.Errorf("expected upload url, got %v", resp.GetUrl())
	}
}

func TestHandleInternalMultipartInit(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	multiGUID := "multipart-guid"
	fileName := "test.bam"
	reqBody := internalapi.InternalMultipartInitRequest{Guid: &multiGUID, FileName: &fileName}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalMultipartInit(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	var resp internalapi.InternalMultipartInitResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(resp.GetGuid()); err != nil {
		t.Fatalf("expected UUID guid, got %q", resp.GetGuid())
	}

	if resp.GetUploadId() != "mock-upload-id" {
		t.Errorf("expected mock-upload-id, got %v", resp.GetUploadId())
	}
}

func TestHandleInternalMultipartInit_MintsUUIDForChecksumInput(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reqBody := internalapi.InternalMultipartInitRequest{FileName: &checksum}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalMultipartInit(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Fatalf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusCreated, rr.Body.String())
	}

	var resp internalapi.InternalMultipartInitResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(resp.GetGuid()); err != nil {
		t.Fatalf("expected minted UUID guid, got %q", resp.GetGuid())
	}
	obj, ok := mockDB.Objects[resp.GetGuid()]
	if !ok {
		t.Fatalf("expected created object for guid %s", resp.GetGuid())
	}
	if len(obj.Checksums) == 0 || obj.Checksums[0].Checksum != checksum {
		t.Fatalf("expected checksum %s to be persisted, got %+v", checksum, obj.Checksums)
	}
}

func TestHandleInternalMultipartInit_ResolvesExistingByChecksumGUID(t *testing.T) {
	checksum := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	existingID := "ee53f5ce-8069-4f99-bd59-0517e6a2f1ea"
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			existingID: {
				Id: existingID,
				Checksums: []drs.Checksum{
					{Type: "sha256", Checksum: checksum},
				},
				AccessMethods: []drs.AccessMethod{
					{
						Type: "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://test-bucket-1/cbds/end_to_end_test/" + checksum,
						},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	reqBody := internalapi.InternalMultipartInitRequest{Guid: &checksum}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalMultipartInit(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Fatalf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusCreated, rr.Body.String())
	}

	var resp internalapi.InternalMultipartInitResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.GetGuid() != existingID {
		t.Fatalf("expected resolved existing UUID guid %s, got %s", existingID, resp.GetGuid())
	}
}

func TestHandleInternalMultipartUpload(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	reqBody := internalapi.InternalMultipartUploadRequest{
		Key:        "hash-key",
		UploadId:   "mock-upload-id",
		PartNumber: 1,
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/multipart/upload", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalMultipartUpload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp internalapi.InternalMultipartUploadResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.GetPresignedUrl() == "" {
		t.Fatal("expected presigned_url to be set")
	}
}

func TestHandleInternalMultipartComplete(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	reqBody := internalapi.InternalMultipartCompleteRequest{
		Key:      "hash-key",
		UploadId: "mock-upload-id",
		Parts: []internalapi.InternalMultipartPart{
			{PartNumber: 1, ETag: "etag1"},
		},
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/internal/data/multipart/complete", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleInternalMultipartComplete(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestHandleInternalDownload_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"secure-id": {
				Id: "secure-id",
				AccessMethods: []drs.AccessMethod{
					{
						Type: "s3",
						AccessUrl: drs.AccessMethodAccessUrl{
							Url: "s3://bucket/key",
						},
					},
				},
			},
		},
		ObjectAuthz: map[string][]string{
			"secure-id": {"/programs/p/projects/q"},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req401, _ := http.NewRequest("GET", "/internal/data/download/secure-id", nil)
	req401 = mux.SetURLVars(req401, map[string]string{"file_id": "secure-id"})
	ctx401 := context.WithValue(req401.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	handleInternalDownload(rr401, req401, mockDB, mockUM)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/internal/data/download/secure-id", nil)
	req403 = mux.SetURLVars(req403, map[string]string{"file_id": "secure-id"})
	ctx403 := context.WithValue(req403.Context(), core.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, core.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	handleInternalDownload(rr403, req403, mockDB, mockUM)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/internal/data/download/secure-id", nil)
	req200 = mux.SetURLVars(req200, map[string]string{"file_id": "secure-id"})
	ctx200 := context.WithValue(req200.Context(), core.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, core.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	handleInternalDownload(rr200, req200, mockDB, mockUM)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}
}

func TestHandleInternalUploadURL_Gen3Unauthorized(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}
	req, _ := http.NewRequest("GET", "/internal/data/upload/some-id?bucket=test-bucket", nil)
	req = mux.SetURLVars(req, map[string]string{"file_id": "some-id"})
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	handleInternalUploadURL(rr, req, mockDB, mockUM)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalBuckets_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"b1": {Bucket: "b1", Region: "us-east-1"},
		},
		BucketScopes: map[string]core.BucketScope{
			"cbds|proj1": {
				Organization: "cbds",
				ProjectID:    "proj1",
				Bucket:       "b1",
				PathPrefix:   "cbds/proj1",
			},
		},
	}

	req401, _ := http.NewRequest("GET", "/internal/data/buckets", nil)
	ctx401 := context.WithValue(req401.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	handleInternalBuckets(rr401, req401, mockDB)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/internal/data/buckets", nil)
	ctx403 := context.WithValue(req403.Context(), core.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, core.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	handleInternalBuckets(rr403, req403, mockDB)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/internal/data/buckets", nil)
	ctx200 := context.WithValue(req200.Context(), core.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, core.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	handleInternalBuckets(rr200, req200, mockDB)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}

	reqScoped, _ := http.NewRequest("GET", "/internal/data/buckets", nil)
	ctxScoped := context.WithValue(reqScoped.Context(), core.AuthModeKey, "gen3")
	ctxScoped = context.WithValue(ctxScoped, core.AuthHeaderPresentKey, true)
	ctxScoped = context.WithValue(ctxScoped, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"read": true},
	})
	reqScoped = reqScoped.WithContext(ctxScoped)
	rrScoped := httptest.NewRecorder()
	handleInternalBuckets(rrScoped, reqScoped, mockDB)
	if rrScoped.Code != http.StatusOK {
		t.Fatalf("expected scoped GET 200, got %d body=%s", rrScoped.Code, rrScoped.Body.String())
	}
	var resp bucketapi.BucketsResponse
	if err := json.Unmarshal(rrScoped.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if _, ok := resp.S3BUCKETS["b1"]; !ok {
		t.Fatalf("expected scoped response to include b1")
	}
}

func TestHandleInternalPutDeleteBucket_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]core.S3Credential{}}
	path := "s3://b2/cbds/proj1"

	putBody, _ := json.Marshal(bucketapi.PutBucketRequest{
		Bucket:       "b2",
		Region:       "us-east-1",
		AccessKey:    "ak",
		SecretKey:    "sk",
		Endpoint:     "https://s3.amazonaws.com",
		Organization: "cbds",
		ProjectId:    "proj1",
		Path:         &path,
	})

	putReq401, _ := http.NewRequest("PUT", "/internal/data/buckets", bytes.NewBuffer(putBody))
	ctxPut401 := context.WithValue(putReq401.Context(), core.AuthModeKey, "gen3")
	ctxPut401 = context.WithValue(ctxPut401, core.AuthHeaderPresentKey, false)
	putReq401 = putReq401.WithContext(ctxPut401)
	putRR401 := httptest.NewRecorder()
	handleInternalPutBucket(putRR401, putReq401, mockDB)
	if putRR401.Code != http.StatusUnauthorized {
		t.Fatalf("expected PUT 401, got %d body=%s", putRR401.Code, putRR401.Body.String())
	}

	putReq201, _ := http.NewRequest("PUT", "/internal/data/buckets", bytes.NewBuffer(putBody))
	ctxPut201 := context.WithValue(putReq201.Context(), core.AuthModeKey, "gen3")
	ctxPut201 = context.WithValue(ctxPut201, core.AuthHeaderPresentKey, true)
	ctxPut201 = context.WithValue(ctxPut201, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"create": true},
	})
	putReq201 = putReq201.WithContext(ctxPut201)
	putRR201 := httptest.NewRecorder()
	handleInternalPutBucket(putRR201, putReq201, mockDB)
	if putRR201.Code != http.StatusCreated {
		t.Fatalf("expected PUT 201, got %d body=%s", putRR201.Code, putRR201.Body.String())
	}

	delReq403, _ := http.NewRequest("DELETE", "/internal/data/buckets/b2", nil)
	delReq403 = mux.SetURLVars(delReq403, map[string]string{"bucket": "b2"})
	ctxDel403 := context.WithValue(delReq403.Context(), core.AuthModeKey, "gen3")
	ctxDel403 = context.WithValue(ctxDel403, core.AuthHeaderPresentKey, true)
	ctxDel403 = context.WithValue(ctxDel403, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"update": true},
	})
	delReq403 = delReq403.WithContext(ctxDel403)
	delRR403 := httptest.NewRecorder()
	handleInternalDeleteBucket(delRR403, delReq403, mockDB)
	if delRR403.Code != http.StatusForbidden {
		t.Fatalf("expected DELETE 403, got %d body=%s", delRR403.Code, delRR403.Body.String())
	}

	delReq204, _ := http.NewRequest("DELETE", "/internal/data/buckets/b2", nil)
	delReq204 = mux.SetURLVars(delReq204, map[string]string{"bucket": "b2"})
	ctxDel204 := context.WithValue(delReq204.Context(), core.AuthModeKey, "gen3")
	ctxDel204 = context.WithValue(ctxDel204, core.AuthHeaderPresentKey, true)
	ctxDel204 = context.WithValue(ctxDel204, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"update": true},
	})
	delReq204 = delReq204.WithContext(ctxDel204)
	delRR204 := httptest.NewRecorder()
	handleInternalDeleteBucket(delRR204, delReq204, mockDB)
	if delRR204.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE 204, got %d body=%s", delRR204.Code, delRR204.Body.String())
	}
}

func TestHandleInternalPutBucket_RejectsInvalidGeneratedPayloads(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]core.S3Credential{}}

	t.Run("missing required project_id", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "/internal/data/buckets", bytes.NewBufferString(`{
			"bucket":"b2",
			"region":"us-east-1",
			"access_key":"ak",
			"secret_key":"sk",
			"endpoint":"https://s3.amazonaws.com",
			"organization":"cbds"
		}`))
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			bucketAdminResource: {"create": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		handleInternalPutBucket(rr, req, mockDB)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "/internal/data/buckets", bytes.NewBufferString(`{
			"bucket":"b2",
			"region":"us-east-1",
			"access_key":"ak",
			"secret_key":"sk",
			"endpoint":"https://s3.amazonaws.com",
			"organization":"cbds",
			"project_id":"proj1",
			"unexpected":"boom"
		}`))
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			bucketAdminResource: {"create": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		handleInternalPutBucket(rr, req, mockDB)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestWriteDBErrorBranches(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	rr401 := httptest.NewRecorder()
	ctx401 := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	writeDBError(rr401, req.WithContext(ctx401), core.ErrUnauthorized)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr401.Code)
	}

	rr404 := httptest.NewRecorder()
	writeDBError(rr404, req, core.ErrNotFound)
	if rr404.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr404.Code)
	}

	rr500 := httptest.NewRecorder()
	writeDBError(rr500, req, errors.New("boom"))
	if rr500.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rr500.Code)
	}
}

func TestHandleInternalUploadURL_Branches(t *testing.T) {
	mockUM := &testutils.MockUrlManager{}

	t.Run("no bucket configured", func(t *testing.T) {
		db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		req := httptest.NewRequest(http.MethodGet, "/internal/data/upload/abc", nil)
		req = mux.SetURLVars(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		handleInternalUploadURL(rr, req, db, mockUM)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 with default mock bucket, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("query bucket and filename signs upload url", func(t *testing.T) {
		db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		req := httptest.NewRequest(http.MethodGet, "/internal/data/upload/abc?bucket=b1&file_name=file.bin&expires_in=60", nil)
		req = mux.SetURLVars(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		handleInternalUploadURL(rr, req, db, mockUM)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "upload=true") {
			t.Fatalf("expected signed upload URL, got %s", rr.Body.String())
		}
	})
}

func TestHandleInternalMultipartValidationErrors(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}

	reqUpload := httptest.NewRequest(http.MethodPost, "/internal/data/multipart/upload", strings.NewReader(`{}`))
	rrUpload := httptest.NewRecorder()
	handleInternalMultipartUpload(rrUpload, reqUpload, db, um)
	if rrUpload.Code != http.StatusBadRequest {
		t.Fatalf("expected upload 400, got %d body=%s", rrUpload.Code, rrUpload.Body.String())
	}

	reqComplete := httptest.NewRequest(http.MethodPost, "/internal/data/multipart/complete", strings.NewReader(`{}`))
	rrComplete := httptest.NewRecorder()
	handleInternalMultipartComplete(rrComplete, reqComplete, db, um)
	if rrComplete.Code != http.StatusBadRequest {
		t.Fatalf("expected complete 400, got %d body=%s", rrComplete.Code, rrComplete.Body.String())
	}
}

func TestRegisterInternalRoutes_Smoke(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}
	router := mux.NewRouter()
	RegisterInternalDataRoutes(router, db, um)

	req := httptest.NewRequest(http.MethodGet, "/internal/data/upload/abc?bucket=b1", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	// No creds configured for b1 in mock -> falls back to signing anyway with mock url manager.
	if rr.Code != http.StatusOK && rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status from registered internal route: %d body=%s", rr.Code, rr.Body.String())
	}
}
