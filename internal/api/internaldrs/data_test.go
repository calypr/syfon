package internaldrs

import (
	"github.com/calypr/syfon/internal/models"

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/bucketapi"
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

func ptr[T any](v T) *T { return &v }

type capturingMultipartURLManager struct {
	key string
}

func withTestAuthzContext(req *http.Request, mode string, privileges map[string]map[string]bool) *http.Request {
	ctx := req.Context()
	switch mode {
	case "gen3":
		ctx = context.WithValue(ctx, common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
	case "local-authz":
		ctx = context.WithValue(ctx, common.AuthModeKey, "local")
		ctx = context.WithValue(ctx, common.AuthzEnforcedKey, true)
	default:
		panic("unknown authz test mode: " + mode)
	}
	ctx = context.WithValue(ctx, common.UserPrivilegesKey, privileges)
	return req.WithContext(ctx)
}

func (m *capturingMultipartURLManager) SignURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	return url, nil
}

func (m *capturingMultipartURLManager) SignUploadURL(ctx context.Context, accessId string, url string, opts urlmanager.SignOptions) (string, error) {
	return url, nil
}

func (m *capturingMultipartURLManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	m.key = key
	return "upload-1", nil
}

func (m *capturingMultipartURLManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	return "", nil
}

func (m *capturingMultipartURLManager) SignDownloadPart(ctx context.Context, accessId string, url string, start int64, end int64, opts urlmanager.SignOptions) (string, error) {
	return "", nil
}

func (m *capturingMultipartURLManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []urlmanager.MultipartPart) error {
	return nil
}

func TestHandleInternalDownload(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-file-id": {
				Id: "test-file-id",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/key"},
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
	req = routeutil.WithPathParams(req, map[string]string{"file_id": "test-file-id"})

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(common.StringVal(resp.Url), "signed=true") {
		t.Errorf("expected signed url, got %v", common.StringVal(resp.Url))
	}
	if len(mockDB.TransferEvents) != 1 {
		t.Fatalf("expected one access-issued event, got %+v", mockDB.TransferEvents)
	}
	ev := mockDB.TransferEvents[0]
	if ev.EventType != models.TransferEventAccessIssued || ev.ObjectID != "test-file-id" || ev.Provider != "s3" || ev.Bucket != "bucket" {
		t.Fatalf("unexpected access-issued event: %+v", ev)
	}
	if ev.AccessGrantID == "" || ev.AccessGrantID == ev.EventID {
		t.Fatalf("expected stable grant id distinct from audit event id: %+v", ev)
	}
}

func TestHandleInternalDownloadPart(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"test-file-id": {
				Id: "test-file-id",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/key"},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	t.Run("success", func(t *testing.T) {
		mockDB.TransferEvents = nil
		req, _ := http.NewRequest("GET", "/data/download/test-file-id/part?start=0&end=1024", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "test-file-id"})

		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownloadPart(rr, req, om)

		if rr.Code != http.StatusOK {
			t.Errorf("expected 200, got %d", rr.Code)
		}

		var resp internalapi.InternalSignedURL
		json.NewDecoder(rr.Body).Decode(&resp)
		if !strings.Contains(common.StringVal(resp.Url), "range=0-1024") {
			t.Errorf("expected signed range url, got %v", common.StringVal(resp.Url))
		}
		if len(mockDB.TransferEvents) != 1 {
			t.Fatalf("expected one ranged access-issued event, got %+v", mockDB.TransferEvents)
		}
		ev := mockDB.TransferEvents[0]
		if ev.EventType != models.TransferEventAccessIssued || ev.RangeStart == nil || ev.RangeEnd == nil || *ev.RangeStart != 0 || *ev.RangeEnd != 1024 || ev.BytesRequested != 1025 {
			t.Fatalf("unexpected ranged access-issued event: %+v", ev)
		}
	})

	t.Run("missing parameters", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/data/download/test-file-id/part?start=0", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "test-file-id"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownloadPart(rr, req, om)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for missing param, got %d", rr.Code)
		}
	})

	t.Run("invalid range", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/data/download/test-file-id/part?start=100&end=50", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "test-file-id"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownloadPart(rr, req, om)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected 400 for invalid range, got %d", rr.Code)
		}
	})

	t.Run("file not found", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/data/download/unknown/part?start=0&end=100", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "unknown"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownloadPart(rr, req, om)
		if rr.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rr.Code)
		}
	})
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
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/cbds/end_to_end_test/" + did + "/" + oid},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req, err := http.NewRequest("GET", "/data/download/"+oid, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = routeutil.WithPathParams(req, map[string]string{"file_id": oid})

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(common.StringVal(resp.Url), "/"+did+"/"+oid) {
		t.Fatalf("expected signed url to include DID-backed key, got %s", common.StringVal(resp.Url))
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
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/cbds/end_to_end_test/" + did},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req, err := http.NewRequest("GET", "/data/download/"+did, nil)
	if err != nil {
		t.Fatal(err)
	}
	req = routeutil.WithPathParams(req, map[string]string{"file_id": did})

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalSignedURL
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(common.StringVal(resp.Url), "/"+did) {
		t.Fatalf("expected signed url to include UUID-backed key, got %s", common.StringVal(resp.Url))
	}
}

func TestHandleInternalDownload_MultiCloud(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"gcs-file": {
				Id: "gcs-file",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeGs,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "gs://gcs-bucket/obj"},
					},
				},
			},
			"azure-file": {
				Id: "azure-file",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodType("azblob"),
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "azblob://azure-bucket/obj"},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	t.Run("gcs", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/data/download/gcs-file", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "gcs-file"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownload(rr, req, om)
		if rr.Code != http.StatusOK {
			t.Errorf("expected 200 for GCS, got %d", rr.Code)
		}
	})

	t.Run("azure", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/data/download/azure-file", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "azure-file"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, mockUM)
		handleInternalDownload(rr, req, om)
		if rr.Code != http.StatusOK {
			t.Errorf("expected 200 for Azure, got %d", rr.Code)
		}
	})
}

func TestHandleInternalUploadBlank(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{},
	}
	mockUM := &testutils.MockUrlManager{}

	guid := "new-guid"
	reqBody := internalapi.InternalUploadBlankRequest{Guid: &guid}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/upload", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalUploadBlank(rr, req, om)

	if status := rr.Code; status != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusCreated)
	}

	var resp internalapi.InternalUploadBlankOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if _, err := uuid.Parse(common.StringVal(resp.Guid)); err != nil {
		t.Fatalf("expected minted UUID guid, got %q", common.StringVal(resp.Guid))
	}
	if !strings.Contains(common.StringVal(resp.Url), "upload=true") {
		t.Errorf("expected upload url, got %v", common.StringVal(resp.Url))
	}
}

func TestHandleInternalMultipartInit(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	multiGUID := "multipart-guid"
	fileName := "test.bam"
	reqBody := internalapi.InternalMultipartInitRequest{Guid: &multiGUID, FileName: &fileName}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartInit(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	bodyBytes := rr.Body.Bytes()
	var resp internalapi.InternalMultipartInitOutput
	if err := json.Unmarshal(bodyBytes, &resp); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(common.StringVal(resp.Guid)); err != nil {
		t.Fatalf("expected UUID guid, got %q", common.StringVal(resp.Guid))
	}

	if common.StringVal(resp.UploadId) != "mock-upload-id" {
		t.Errorf("expected mock-upload-id, got %v", common.StringVal(resp.UploadId))
	}

	var raw map[string]any
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		t.Fatalf("failed to decode raw response: %v", err)
	}
	if _, ok := raw["uploadId"]; !ok {
		t.Fatalf("expected canonical uploadId field in response, got %v", raw)
	}
	if _, ok := raw["upload_id"]; ok {
		t.Fatalf("unexpected legacy upload_id field in response, got %v", raw)
	}
}

func TestHandleInternalMultipartInit_PreservesRequestedKey(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &capturingMultipartURLManager{}

	key := "programs/programs/projects/e2e/sha256-value"
	reqBody := internalapi.InternalMultipartInitRequest{Guid: &key}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartInit(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}
	if mockUM.key != key {
		t.Fatalf("expected multipart init key %q, got %q", key, mockUM.key)
	}
}

func TestHandleInternalMultipartInit_MintsUUIDForChecksumInput(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}

	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reqBody := internalapi.InternalMultipartInitRequest{FileName: &checksum}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartInit(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalMultipartInitOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(common.StringVal(resp.Guid)); err != nil {
		t.Fatalf("expected minted UUID guid, got %q", common.StringVal(resp.Guid))
	}
	obj, ok := mockDB.Objects[common.StringVal(resp.Guid)]
	if !ok {
		t.Fatalf("expected created object for guid %s", common.StringVal(resp.Guid))
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
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://test-bucket-1/cbds/end_to_end_test/" + checksum},
					},
				},
			},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	reqBody := internalapi.InternalMultipartInitRequest{Guid: &checksum}
	body, _ := json.Marshal(reqBody)
	req, err := http.NewRequest("POST", "/data/multipart/init", bytes.NewBuffer(body))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartInit(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v body=%s", status, http.StatusOK, rr.Body.String())
	}

	var resp internalapi.InternalMultipartInitOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if common.StringVal(resp.Guid) != existingID {
		t.Fatalf("expected resolved existing UUID guid %s, got %s", existingID, common.StringVal(resp.Guid))
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
	req, err := http.NewRequest("POST", "/data/multipart/upload", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartUpload(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	var resp internalapi.InternalMultipartUploadOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if common.StringVal(resp.PresignedUrl) == "" {
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
	req, err := http.NewRequest("POST", "/data/multipart/complete", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalMultipartComplete(rr, req, om)

	if status := rr.Code; status != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestHandleInternalDownload_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"secure-id": {
				Id: "secure-id",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://bucket/key"},
					},
				},
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"secure-id": {"p": {"q"}},
		},
	}
	mockUM := &testutils.MockUrlManager{}

	req401, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req401 = routeutil.WithPathParams(req401, map[string]string{"file_id": "secure-id"})
	ctx401 := context.WithValue(req401.Context(), common.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, common.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr401, req401, om)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req403 = routeutil.WithPathParams(req403, map[string]string{"file_id": "secure-id"})
	ctx403 := context.WithValue(req403.Context(), common.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, common.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr403, req403, om)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
	req200 = routeutil.WithPathParams(req200, map[string]string{"file_id": "secure-id"})
	ctx200 := context.WithValue(req200.Context(), common.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, common.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/p/projects/q": {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, mockUM)
	handleInternalDownload(rr200, req200, om)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}
}

func TestHandleInternalDownload_AuthzParity(t *testing.T) {
	for _, mode := range []string{"gen3", "local-authz"} {
		t.Run(mode, func(t *testing.T) {
			mockDB := &testutils.MockDatabase{
				Objects: map[string]*drs.DrsObject{
					"secure-id": {
						Id: "secure-id",
						AccessMethods: &[]drs.AccessMethod{
							{
								Type: drs.AccessMethodTypeS3,
								AccessUrl: &struct {
									Headers *[]string `json:"headers,omitempty"`
									Url     string    `json:"url"`
								}{Url: "s3://bucket/key"},
							},
						},
					},
				},
				ObjectAuthz: map[string]map[string][]string{
					"secure-id": {"p": {"q"}},
				},
			}
			om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})

			req403, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
			req403 = routeutil.WithPathParams(req403, map[string]string{"file_id": "secure-id"})
			req403 = withTestAuthzContext(req403, mode, map[string]map[string]bool{
				"/programs/p/projects/q": {"create": true},
			})
			rr403 := httptest.NewRecorder()
			handleInternalDownload(rr403, req403, om)
			if rr403.Code != http.StatusForbidden {
				t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
			}

			req200, _ := http.NewRequest("GET", "/data/download/secure-id", nil)
			req200 = routeutil.WithPathParams(req200, map[string]string{"file_id": "secure-id"})
			req200 = withTestAuthzContext(req200, mode, map[string]map[string]bool{
				"/programs/p/projects/q": {"read": true},
			})
			rr200 := httptest.NewRecorder()
			handleInternalDownload(rr200, req200, om)
			if rr200.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
			}
		})
	}
}

func TestHandleInternalUploadURL_Gen3Unauthorized(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	mockUM := &testutils.MockUrlManager{}
	req, _ := http.NewRequest("GET", "/data/upload/some-id?bucket=test-bucket", nil)
	req = routeutil.WithPathParams(req, map[string]string{"file_id": "some-id"})
	ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, mockUM)
	handleInternalUploadURL(rr, req, om)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleInternalBuckets_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"b1": {Bucket: "b1", Region: "us-east-1"},
		},
		BucketScopes: map[string]models.BucketScope{
			"cbds|proj1": {
				Organization: "cbds",
				ProjectID:    "proj1",
				Bucket:       "b1",
				PathPrefix:   "cbds/proj1",
			},
		},
	}

	req401, err := http.NewRequest("GET", "/data/buckets", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx401 := context.WithValue(req401.Context(), common.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, common.AuthHeaderPresentKey, false)
	req401 = req401.WithContext(ctx401)
	rr401 := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBuckets(rr401, req401, om)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d body=%s", rr401.Code, rr401.Body.String())
	}

	req403, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctx403 := context.WithValue(req403.Context(), common.AuthModeKey, "gen3")
	ctx403 = context.WithValue(ctx403, common.AuthHeaderPresentKey, true)
	ctx403 = context.WithValue(ctx403, common.UserPrivilegesKey, map[string]map[string]bool{
		bucketControlResource: {"create": true},
	})
	req403 = req403.WithContext(ctx403)
	rr403 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBuckets(rr403, req403, om)
	if rr403.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rr403.Code, rr403.Body.String())
	}

	req200, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctx200 := context.WithValue(req200.Context(), common.AuthModeKey, "gen3")
	ctx200 = context.WithValue(ctx200, common.AuthHeaderPresentKey, true)
	ctx200 = context.WithValue(ctx200, common.UserPrivilegesKey, map[string]map[string]bool{
		bucketControlResource: {"read": true},
	})
	req200 = req200.WithContext(ctx200)
	rr200 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBuckets(rr200, req200, om)
	if rr200.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr200.Code, rr200.Body.String())
	}

	reqScoped, _ := http.NewRequest("GET", "/data/buckets", nil)
	ctxScoped := context.WithValue(reqScoped.Context(), common.AuthModeKey, "gen3")
	ctxScoped = context.WithValue(ctxScoped, common.AuthHeaderPresentKey, true)
	ctxScoped = context.WithValue(ctxScoped, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"read": true},
	})
	reqScoped = reqScoped.WithContext(ctxScoped)
	rrScoped := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBuckets(rrScoped, reqScoped, om)
	if rrScoped.Code != http.StatusOK {
		t.Fatalf("expected scoped GET 200, got %d body=%s", rrScoped.Code, rrScoped.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rrScoped.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	bucketsRaw, ok := resp["S3_BUCKETS"].(map[string]any)
	if !ok {
		bucketsRaw, ok = resp["S3BUCKETS"].(map[string]any)
	}
	if !ok {
		t.Fatalf("expected S3_BUCKETS object in response, got %v", resp)
	}
	if _, ok := bucketsRaw["b1"]; !ok {
		t.Fatalf("expected scoped response to include b1")
	}
}

func TestHandleInternalPutDeleteBucket_Gen3Auth(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]models.S3Credential{}}
	path := "s3://bucket2/cbds/proj1"

	region := "us-east-1"
	accessKey := "ak"
	secretKey := "sk"
	endpoint := t.TempDir()
	provider := "file"
	putBody, _ := json.Marshal(bucketapi.PutBucketRequest{
		Bucket:       "bucket2",
		Provider:     &provider,
		Region:       &region,
		AccessKey:    &accessKey,
		SecretKey:    &secretKey,
		Endpoint:     &endpoint,
		Organization: "cbds",
		ProjectId:    "proj1",
		Path:         &path,
	})

	putReq401, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBuffer(putBody))
	ctxPut401 := context.WithValue(putReq401.Context(), common.AuthModeKey, "gen3")
	ctxPut401 = context.WithValue(ctxPut401, common.AuthHeaderPresentKey, false)
	putReq401 = putReq401.WithContext(ctxPut401)
	putRR401 := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalPutBucket(putRR401, putReq401, om)
	if putRR401.Code != http.StatusUnauthorized {
		t.Fatalf("expected PUT 401, got %d body=%s", putRR401.Code, putRR401.Body.String())
	}

	putReq201, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBuffer(putBody))
	ctxPut201 := context.WithValue(putReq201.Context(), common.AuthModeKey, "gen3")
	ctxPut201 = context.WithValue(ctxPut201, common.AuthHeaderPresentKey, true)
	ctxPut201 = context.WithValue(ctxPut201, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"create": true},
	})
	putReq201 = putReq201.WithContext(ctxPut201)
	putRR201 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalPutBucket(putRR201, putReq201, om)
	if putRR201.Code != http.StatusCreated {
		t.Fatalf("expected PUT 201, got %d body=%s", putRR201.Code, putRR201.Body.String())
	}

	// Extend the same credential to a second scope without resupplying secrets.
	putScopeOnlyReq, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBufferString(`{
		"bucket":"bucket2",
		"organization":"cbds",
		"project_id":"proj2"
	}`))
	ctxPutScopeOnly := context.WithValue(putScopeOnlyReq.Context(), common.AuthModeKey, "gen3")
	ctxPutScopeOnly = context.WithValue(ctxPutScopeOnly, common.AuthHeaderPresentKey, true)
	ctxPutScopeOnly = context.WithValue(ctxPutScopeOnly, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj2": {"create": true},
	})
	putScopeOnlyReq = putScopeOnlyReq.WithContext(ctxPutScopeOnly)
	putScopeOnlyRR := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalPutBucket(putScopeOnlyRR, putScopeOnlyReq, om)
	if putScopeOnlyRR.Code != http.StatusCreated {
		t.Fatalf("expected scope-only PUT 201, got %d body=%s", putScopeOnlyRR.Code, putScopeOnlyRR.Body.String())
	}

	// Dedicated scope extension endpoint.
	postScopeReq, _ := http.NewRequest("POST", "/data/buckets/bucket2/scopes", bytes.NewBufferString(`{
		"organization":"cbds",
		"project_id":"proj3"
	}`))
	postScopeReq = routeutil.WithPathParams(postScopeReq, map[string]string{"bucket": "bucket2"})
	ctxPostScope := context.WithValue(postScopeReq.Context(), common.AuthModeKey, "gen3")
	ctxPostScope = context.WithValue(ctxPostScope, common.AuthHeaderPresentKey, true)
	ctxPostScope = context.WithValue(ctxPostScope, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj3": {"create": true},
	})
	postScopeReq = postScopeReq.WithContext(ctxPostScope)
	postScopeRR := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalCreateBucketScope(postScopeRR, postScopeReq, om)
	if postScopeRR.Code != http.StatusCreated {
		t.Fatalf("expected scope POST 201, got %d body=%s", postScopeRR.Code, postScopeRR.Body.String())
	}

	delReq403, _ := http.NewRequest("DELETE", "/data/buckets/bucket2", nil)
	delReq403 = routeutil.WithPathParams(delReq403, map[string]string{"bucket": "bucket2"})
	ctxDel403 := context.WithValue(delReq403.Context(), common.AuthModeKey, "gen3")
	ctxDel403 = context.WithValue(ctxDel403, common.AuthHeaderPresentKey, true)
	ctxDel403 = context.WithValue(ctxDel403, common.UserPrivilegesKey, map[string]map[string]bool{
		bucketControlResource: {"update": true},
	})
	delReq403 = delReq403.WithContext(ctxDel403)
	delRR403 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalDeleteBucket(delRR403, delReq403, om)
	if delRR403.Code != http.StatusForbidden {
		t.Fatalf("expected DELETE 403, got %d body=%s", delRR403.Code, delRR403.Body.String())
	}

	delReq204, _ := http.NewRequest("DELETE", "/data/buckets/bucket2", nil)
	delReq204 = routeutil.WithPathParams(delReq204, map[string]string{"bucket": "bucket2"})
	ctxDel204 := context.WithValue(delReq204.Context(), common.AuthModeKey, "gen3")
	ctxDel204 = context.WithValue(ctxDel204, common.AuthHeaderPresentKey, true)
	ctxDel204 = context.WithValue(ctxDel204, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/cbds/projects/proj1": {"update": true},
		"/programs/cbds/projects/proj2": {"update": true},
		"/programs/cbds/projects/proj3": {"update": true},
	})
	delReq204 = delReq204.WithContext(ctxDel204)
	delRR204 := httptest.NewRecorder()
	om = core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalDeleteBucket(delRR204, delReq204, om)
	if delRR204.Code != http.StatusNoContent {
		t.Fatalf("expected DELETE 204, got %d body=%s", delRR204.Code, delRR204.Body.String())
	}
}

func TestHandleInternalPutBucket_RejectsInvalidGeneratedPayloads(t *testing.T) {
	mockDB := &testutils.MockDatabase{Credentials: map[string]models.S3Credential{}}

	t.Run("missing required project_id", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBufferString(`{
			"bucket":"b2",
			"region":"us-east-1",
			"access_key":"ak",
			"secret_key":"sk",
			"endpoint":"https://s3.amazonaws.com",
			"organization":"cbds"
		}`))
		ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			bucketControlResource: {"create": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalPutBucket(rr, req, om)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		req, _ := http.NewRequest("PUT", "/data/buckets", bytes.NewBufferString(`{
			"bucket":"b2",
			"region":"us-east-1",
			"access_key":"ak",
			"secret_key":"sk",
			"endpoint":"https://s3.amazonaws.com",
			"organization":"cbds",
			"project_id":"proj1",
			"unexpected":"boom"
		}`))
		ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			bucketControlResource: {"create": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalPutBucket(rr, req, om)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestWriteDBErrorBranches(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	rr401 := httptest.NewRecorder()
	ctx401 := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
	ctx401 = context.WithValue(ctx401, common.AuthHeaderPresentKey, false)
	writeDBError(rr401, req.WithContext(ctx401), common.ErrUnauthorized)
	if rr401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr401.Code)
	}

	rr404 := httptest.NewRecorder()
	writeDBError(rr404, req, common.ErrNotFound)
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

	t.Run("no_bucket_configured", func(t *testing.T) {
		db := &testutils.MockDatabase{
			Objects:     map[string]*drs.DrsObject{},
			Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}},
		}
		req := httptest.NewRequest(http.MethodGet, "/data/upload/abc", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(db, mockUM)
		handleInternalUploadURL(rr, req, om)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200 with default mock bucket, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("query_bucket_and_filename_signs_upload_url", func(t *testing.T) {
		db := &testutils.MockDatabase{
			Objects:     map[string]*drs.DrsObject{},
			Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}},
		}
		req := httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1&filename=f1", nil)
		req = routeutil.WithPathParams(req, map[string]string{"file_id": "abc"})
		rr := httptest.NewRecorder()
		om := core.NewObjectManager(db, mockUM)
		handleInternalUploadURL(rr, req, om)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "upload=true") {
			t.Fatalf("expected signed upload URL, got %s", rr.Body.String())
		}
	})
}

func TestHandleInternalUploadBulk_MixedResults(t *testing.T) {
	mockUM := &testutils.MockUrlManager{}
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {
				Id: "obj-1",
				AccessMethods: &[]drs.AccessMethod{
					{
						Type: drs.AccessMethodTypeS3,
						AccessUrl: &struct {
							Headers *[]string `json:"headers,omitempty"`
							Url     string    `json:"url"`
						}{Url: "s3://b1/prefix/from-existing.bin"},
					},
				},
			},
		},
		Credentials: map[string]models.S3Credential{
			"b1": {Bucket: "b1", Provider: "s3", Region: "us-east-1"},
		},
	}

	reqBody := internalapi.InternalUploadBulkRequest{
		Requests: []internalapi.InternalUploadBulkItem{
			{FileId: "obj-1", Bucket: ptr("b1")},
			{FileId: ""},
		},
	}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/data/upload/bulk", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	om := core.NewObjectManager(db, mockUM)

	handleInternalUploadBulk(rr, req, om)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207 for mixed batch results, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp internalapi.InternalUploadBulkOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(*resp.Results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(*resp.Results))
	}
	if (*resp.Results)[0].Status != int32(http.StatusOK) || (*resp.Results)[0].Url == nil {
		t.Fatalf("expected first result to be signed, got %+v", (*resp.Results)[0])
	}
	if !strings.Contains(*(*resp.Results)[0].Url, "upload=true") {
		t.Fatalf("expected signed upload URL in first result, got %+v", (*resp.Results)[0])
	}
	if common.StringVal((*resp.Results)[0].FileName) != "prefix/from-existing.bin" {
		t.Fatalf("expected resolved key from existing object, got %q", common.StringVal((*resp.Results)[0].FileName))
	}
	if (*resp.Results)[1].Status != int32(http.StatusBadRequest) {
		t.Fatalf("expected second result 400, got %+v", (*resp.Results)[1])
	}
}

func TestHandleInternalUploadBulk_Gen3UnauthorizedPerItem(t *testing.T) {
	mockUM := &testutils.MockUrlManager{}
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"secure-id": {
				Id: "secure-id",
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"secure-id": {"p": {"q"}},
		},
	}

	reqBody := internalapi.InternalUploadBulkRequest{
		Requests: []internalapi.InternalUploadBulkItem{
			{FileId: "secure-id", Bucket: ptr("b1")},
		},
	}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/data/upload/bulk", bytes.NewReader(body))
	ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, false)
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	om := core.NewObjectManager(db, mockUM)
	handleInternalUploadBulk(rr, req, om)

	if rr.Code != http.StatusMultiStatus {
		t.Fatalf("expected 207, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp internalapi.InternalUploadBulkOutput
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(*resp.Results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(*resp.Results))
	}
	if (*resp.Results)[0].Status != int32(http.StatusUnauthorized) {
		t.Fatalf("expected per-item status 401, got %+v", (*resp.Results)[0])
	}
}

func TestHandleInternalMultipartValidationErrors(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	um := &testutils.MockUrlManager{}

	reqUpload := httptest.NewRequest(http.MethodPost, "/data/multipart/upload", strings.NewReader(`{}`))
	rrUpload := httptest.NewRecorder()
	om := core.NewObjectManager(db, um)
	handleInternalMultipartUpload(rrUpload, reqUpload, om)
	if rrUpload.Code != http.StatusBadRequest {
		t.Fatalf("expected upload 400, got %d body=%s", rrUpload.Code, rrUpload.Body.String())
	}

	reqComplete := httptest.NewRequest(http.MethodPost, "/data/multipart/complete", strings.NewReader(`{}`))
	rrComplete := httptest.NewRecorder()
	om = core.NewObjectManager(db, um)
	handleInternalMultipartComplete(rrComplete, reqComplete, om)
	if rrComplete.Code != http.StatusBadRequest {
		t.Fatalf("expected complete 400, got %d body=%s", rrComplete.Code, rrComplete.Body.String())
	}

	// Strict contract: only uploadId is accepted as query fallback.
	reqLegacyUpload := httptest.NewRequest(http.MethodPost, "/data/multipart/upload?key=k&upload_id=u&partNumber=1", nil)
	rrLegacyUpload := httptest.NewRecorder()
	om = core.NewObjectManager(db, um)
	handleInternalMultipartUpload(rrLegacyUpload, reqLegacyUpload, om)
	if rrLegacyUpload.Code != http.StatusBadRequest {
		t.Fatalf("expected legacy upload_id upload request to fail with 400, got %d body=%s", rrLegacyUpload.Code, rrLegacyUpload.Body.String())
	}

	reqLegacyComplete := httptest.NewRequest(http.MethodPost, "/data/multipart/complete?key=k&upload_id=u", nil)
	rrLegacyComplete := httptest.NewRecorder()
	om = core.NewObjectManager(db, um)
	handleInternalMultipartComplete(rrLegacyComplete, reqLegacyComplete, om)
	if rrLegacyComplete.Code != http.StatusBadRequest {
		t.Fatalf("expected legacy upload_id complete request to fail with 400, got %d body=%s", rrLegacyComplete.Code, rrLegacyComplete.Body.String())
	}
}

func TestRegisterInternalRoutes_Smoke(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects:     map[string]*drs.DrsObject{},
		Credentials: map[string]models.S3Credential{"b1": {Bucket: "b1"}},
	}
	um := &testutils.MockUrlManager{}
	app := fiber.New()
	om := core.NewObjectManager(db, um)
	RegisterInternalDataRoutes(app, om)

	req := httptest.NewRequest(http.MethodGet, "/data/upload/abc?bucket=b1", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	// No creds configured for b1 in mock -> falls back to signing anyway with mock url manager.
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("unexpected status from registered internal route: %d body=%s", resp.StatusCode, string(body))
	}
}
