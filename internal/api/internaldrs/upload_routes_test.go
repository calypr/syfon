package internaldrs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/google/uuid"
)

func TestHandleInternalUploadBlank(t *testing.T) {
	guid := "new-guid"
	body, _ := json.Marshal(internalapi.InternalUploadBlankRequest{Guid: &guid})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/upload", bytes.NewBuffer(body)), core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}
	var resp internalapi.InternalUploadBlankOutput
	_ = json.NewDecoder(rr.Body).Decode(&resp)
	if _, err := uuid.Parse(common.StringVal(resp.Guid)); err != nil {
		t.Fatalf("expected minted UUID, got %q", common.StringVal(resp.Guid))
	}
}

func TestHandleInternalMultipartInit(t *testing.T) {
	fileName := "test.bam"
	guid := "multipart-guid"
	body, _ := json.Marshal(internalapi.InternalMultipartInitRequest{Guid: &guid, FileName: &fileName})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/init", bytes.NewBuffer(body)), core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleInternalMultipartInit_PreservesRequestedKey(t *testing.T) {
	key := "programs/programs/projects/e2e/sha256-value"
	body, _ := json.Marshal(internalapi.InternalMultipartInitRequest{Guid: &key})
	mockUM := &capturingMultipartURLManager{}
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/init", bytes.NewBuffer(body)), core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, mockUM))
	if rr.Code != http.StatusOK || mockUM.key != key {
		t.Fatalf("expected preserved key, got status=%d key=%q", rr.Code, mockUM.key)
	}
}

func TestHandleInternalMultipartInit_MintsUUIDForChecksumInput(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	body, _ := json.Marshal(internalapi.InternalMultipartInitRequest{FileName: &checksum})
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/init", bytes.NewBuffer(body)), core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleInternalMultipartInit_ResolvesExistingByChecksumGUID(t *testing.T) {
	checksum := strings.Repeat("b", 64)
	existingID := "ee53f5ce-8069-4f99-bd59-0517e6a2f1ea"
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			existingID: {Id: existingID, Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}}},
		},
	}
	body, _ := json.Marshal(internalapi.InternalMultipartInitRequest{Guid: &checksum})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/init", bytes.NewBuffer(body)), core.NewObjectManager(mockDB, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleInternalMultipartUpload(t *testing.T) {
	body, _ := json.Marshal(internalapi.InternalMultipartUploadRequest{Key: "hash-key", UploadId: "mock-upload-id", PartNumber: 1})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/upload", bytes.NewBuffer(body)), core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleInternalMultipartComplete(t *testing.T) {
	body, _ := json.Marshal(internalapi.InternalMultipartCompleteRequest{Key: "hash-key", UploadId: "mock-upload-id", Parts: []internalapi.InternalMultipartPart{{PartNumber: 1, ETag: "etag1"}}})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/complete", bytes.NewBuffer(body)), core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}

func TestHandleInternalUploadURL_Gen3Unauthorized(t *testing.T) {
	req := routeutil.WithPathParams(httptest.NewRequest(http.MethodGet, "/data/upload/some-id?bucket=test-bucket", nil), map[string]string{"file_id": "some-id"})
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", false, nil))
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
}

func TestHandleInternalUploadURL_Branches(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}, Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}}}
	req := routeutil.WithPathParams(httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1&filename=f1", nil), map[string]string{"file_id": "abc"})
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(db, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "upload=true") {
		t.Fatalf("expected signed upload URL, got status=%d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalUploadBulk_MixedResults(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{"obj-1": {Id: "obj-1", AccessMethods: &[]drs.AccessMethod{{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: "s3://b1/prefix/from-existing.bin"}}}}},
		Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1", Provider: "s3", Region: "us-east-1"}},
	}
	body, _ := json.Marshal(internalapi.InternalUploadBulkRequest{Requests: []internalapi.InternalUploadBulkItem{{FileId: "obj-1", Bucket: ptr("b1")}, {FileId: ""}}})
	rr := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/upload/bulk", bytes.NewBuffer(body)), core.NewObjectManager(db, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
}

func TestHandleInternalUploadBulk_Gen3UnauthorizedPerItem(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects:     map[string]*drs.DrsObject{"secure-id": {Id: "secure-id"}},
		ObjectAuthz: map[string]map[string][]string{"secure-id": {"p": {"q"}}},
	}
	body, _ := json.Marshal(internalapi.InternalUploadBulkRequest{Requests: []internalapi.InternalUploadBulkItem{{FileId: "secure-id", Bucket: ptr("b1")}}})
	req := httptest.NewRequest(http.MethodPost, "/data/upload/bulk", bytes.NewBuffer(body))
	req = req.WithContext(dataTestAuthContext(req.Context(), "gen3", false, nil))
	rr := doInternalDRSTestRequest(req, core.NewObjectManager(db, &testutils.MockUrlManager{}))
	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d", rr.Code)
	}
}

func TestHandleInternalMultipartValidationErrors(t *testing.T) {
	om := core.NewObjectManager(&testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}, &testutils.MockUrlManager{})
	rrUpload := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/upload", strings.NewReader(`{}`)), om)
	if rrUpload.Code != http.StatusBadRequest {
		t.Fatalf("expected upload 400, got %d", rrUpload.Code)
	}
	rrComplete := doInternalDRSTestRequest(httptest.NewRequest(http.MethodPost, "/data/multipart/complete", strings.NewReader(`{}`)), om)
	if rrComplete.Code != http.StatusBadRequest {
		t.Fatalf("expected complete 400, got %d", rrComplete.Code)
	}
}
