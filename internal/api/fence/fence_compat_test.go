package fence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
	"github.com/gorilla/mux"
)

func TestHandleFenceDownload(t *testing.T) {
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

	req, err := http.NewRequest("GET", "/data/download/test-file-id", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = mux.SetURLVars(req, map[string]string{"file_id": "test-file-id"})

	rr := httptest.NewRecorder()
	handleFenceDownload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp fenceSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(resp.URL, "signed=true") {
		t.Errorf("expected signed url, got %v", resp.URL)
	}
}

func TestHandleFenceUploadBlank(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	mockUM := &testutils.MockUrlManager{}

	reqBody := fenceUploadBlankRequest{
		GUID: "new-guid",
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/upload", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleFenceUploadBlank(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	var resp fenceUploadBlankResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if resp.GUID != "new-guid" {
		t.Errorf("expected guid new-guid, got %v", resp.GUID)
	}
	if !strings.Contains(resp.URL, "upload=true") {
		t.Errorf("expected upload url, got %v", resp.URL)
	}
}

func TestHandleFenceMultipartInit(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	reqBody := fenceMultipartInitRequest{
		GUID:     "multipart-guid",
		FileName: "test.bam",
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleFenceMultipartInit(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	var resp fenceMultipartInitResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if resp.UploadID != "mock-upload-id" {
		t.Errorf("expected mock-upload-id, got %v", resp.UploadID)
	}
}

func TestHandleFenceMultipartUpload(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	reqBody := fenceMultipartUploadRequest{
		Key:        "hash-key",
		UploadID:   "mock-upload-id",
		PartNumber: 1,
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/user/data/multipart/upload", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleFenceMultipartUpload(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp fenceMultipartUploadResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.PresignedURL == "" {
		t.Fatal("expected presigned_url to be set")
	}
}

func TestHandleFenceMultipartComplete(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	reqBody := fenceMultipartCompleteRequest{
		Key:      "hash-key",
		UploadID: "mock-upload-id",
		Parts: []fenceMultipartPart{
			{PartNumber: 1, ETag: "etag1"},
		},
	}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/user/data/multipart/complete", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handleFenceMultipartComplete(rr, req, mockDB, mockUM)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestHandleFenceDownload_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"secure-id": {
				Id:             "secure-id",
				Authorizations: []string{"/programs/p/projects/q"},
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

	req401, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req401 = mux.SetURLVars(req401, map[string]string{"file_id": "secure-id"})
	ctx401 := context.WithValue(req401.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	handleFenceDownload(rr401, req401, mockDB, mockUM)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req403 = mux.SetURLVars(req403, map[string]string{"file_id": "secure-id"})
	ctx403 := context.WithValue(req403.Context(), core.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, core.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	handleFenceDownload(rr403, req403, mockDB, mockUM)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req200 = mux.SetURLVars(req200, map[string]string{"file_id": "secure-id"})
	ctx200 := context.WithValue(req200.Context(), core.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, core.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	handleFenceDownload(rr200, req200, mockDB, mockUM)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}
}

func TestHandleFenceUploadURL_Gen3Unauthorized(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}
	req, _ := http.NewRequest("GET", "/data/upload/some-id?bucket=test-bucket", nil)
	req = mux.SetURLVars(req, map[string]string{"file_id": "some-id"})
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	handleFenceUploadURL(rr, req, mockDB, mockUM)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleFenceBuckets_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]core.S3Credential{
			"b1": {Bucket: "b1", Region: "us-east-1"},
		},
	}

	req401, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctx401 := context.WithValue(req401.Context(), core.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, core.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	handleFenceBuckets(rr401, req401, mockDB)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctx403 := context.WithValue(req403.Context(), core.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, core.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	handleFenceBuckets(rr403, req403, mockDB)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctx200 := context.WithValue(req200.Context(), core.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, core.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	handleFenceBuckets(rr200, req200, mockDB)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}
}

func TestHandleFencePutDeleteBucket_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]core.S3Credential{}}

	putBody, _ := json.Marshal(fencePutBucketRequest{
		Bucket:    "b2",
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
	})

	putReq401, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBuffer(putBody))
	ctxPut401 := context.WithValue(putReq401.Context(), core.AuthModeKey, "gen3")
	ctxPut401 = context.WithValue(ctxPut401, core.AuthHeaderPresentKey, false)
	putReq401 = putReq401.WithContext(ctxPut401)
	putRR401 := httptest.NewRecorder()
	handleFencePutBucket(putRR401, putReq401, mockDB)
	if putRR401.Code != http.StatusUnauthorized {
		t.Fatalf("expected PUT 401, got %d body=%s", putRR401.Code, putRR401.Body.String())
	}

	putReq201, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBuffer(putBody))
	ctxPut201 := context.WithValue(putReq201.Context(), core.AuthModeKey, "gen3")
	ctxPut201 = context.WithValue(ctxPut201, core.AuthHeaderPresentKey, true)
	ctxPut201 = context.WithValue(ctxPut201, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"create": true},
	})
	putReq201 = putReq201.WithContext(ctxPut201)
	putRR201 := httptest.NewRecorder()
	handleFencePutBucket(putRR201, putReq201, mockDB)
	if putRR201.Code != http.StatusCreated {
		t.Fatalf("expected PUT 201, got %d body=%s", putRR201.Code, putRR201.Body.String())
	}

	delReq403, _ := http.NewRequest("DELETE", "/data/buckets/b2", nil)
	delReq403 = mux.SetURLVars(delReq403, map[string]string{"bucket": "b2"})
	ctxDel403 := context.WithValue(delReq403.Context(), core.AuthModeKey, "gen3")
	ctxDel403 = context.WithValue(ctxDel403, core.AuthHeaderPresentKey, true)
	ctxDel403 = context.WithValue(ctxDel403, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"update": true},
	})
	delReq403 = delReq403.WithContext(ctxDel403)
	delRR403 := httptest.NewRecorder()
	handleFenceDeleteBucket(delRR403, delReq403, mockDB)
	if delRR403.Code != http.StatusForbidden {
		t.Fatalf("expected DELETE 403, got %d body=%s", delRR403.Code, delRR403.Body.String())
	}

	delReq204, _ := http.NewRequest("DELETE", "/data/buckets/b2", nil)
	delReq204 = mux.SetURLVars(delReq204, map[string]string{"bucket": "b2"})
	ctxDel204 := context.WithValue(delReq204.Context(), core.AuthModeKey, "gen3")
	ctxDel204 = context.WithValue(ctxDel204, core.AuthHeaderPresentKey, true)
	ctxDel204 = context.WithValue(ctxDel204, core.UserPrivilegesKey, map[string]map[string]bool{
		bucketAdminResource: {"delete": true},
	})
	delReq204 = delReq204.WithContext(ctxDel204)
	delRR204 := httptest.NewRecorder()
	handleFenceDeleteBucket(delRR204, delReq204, mockDB)
	if delRR204.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE 204, got %d body=%s", delRR204.Code, delRR204.Body.String())
	}
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

func TestHandleFenceUploadURL_Branches(t *testing.T) {
	mockUM := &testutils.MockUrlManager{}

	t.Run("no bucket configured", func(t *testing.T) {
		db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		req := httptest.NewRequest(http.MethodGet, "/data/upload/abc", nil)
		req = mux.SetURLVars(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		handleFenceUploadURL(rr, req, db, mockUM)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 with default mock bucket, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("query bucket and filename signs upload url", func(t *testing.T) {
		db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		req := httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1&file_name=file.bin&expires_in=60", nil)
		req = mux.SetURLVars(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		handleFenceUploadURL(rr, req, db, mockUM)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "upload=true") {
			t.Fatalf("expected signed upload URL, got %s", rr.Body.String())
		}
	})
}

func TestHandleFenceMultipartValidationErrors(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}

	reqUpload := httptest.NewRequest(http.MethodPost, "/user/data/multipart/upload", strings.NewReader(`{}`))
	rrUpload := httptest.NewRecorder()
	handleFenceMultipartUpload(rrUpload, reqUpload, db, um)
	if rrUpload.Code != http.StatusBadRequest {
		t.Fatalf("expected upload 400, got %d body=%s", rrUpload.Code, rrUpload.Body.String())
	}

	reqComplete := httptest.NewRequest(http.MethodPost, "/user/data/multipart/complete", strings.NewReader(`{}`))
	rrComplete := httptest.NewRecorder()
	handleFenceMultipartComplete(rrComplete, reqComplete, db, um)
	if rrComplete.Code != http.StatusBadRequest {
		t.Fatalf("expected complete 400, got %d body=%s", rrComplete.Code, rrComplete.Body.String())
	}
}

func TestRegisterFenceRoutes_Smoke(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}
	router := mux.NewRouter()
	RegisterFenceRoutes(router, db, um)

	req := httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	// No creds configured for b1 in mock -> falls back to signing anyway with mock url manager.
	if rr.Code != http.StatusOK && rr.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status from registered fence route: %d body=%s", rr.Code, rr.Body.String())
	}
}
