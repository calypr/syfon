package fence

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
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
