package transfers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestHandleInternalMultipartUpload_NotFound(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &internalDRSStorageFake{}
	om := newInternalDRSObjectManager(mockDB, mockUM)
	app := fiber.New()
	app.Post("/multipart/upload", handleInternalMultipartUploadFiber(om.TransferService))

	reqBody := internalapi.InternalMultipartUploadRequest{
		UploadId:   "non-existent",
		PartNumber: 1,
	}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/multipart/upload", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, _ := app.Test(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHandleInternalMultipartComplete_NotFound(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &internalDRSStorageFake{}
	om := newInternalDRSObjectManager(mockDB, mockUM)
	app := fiber.New()
	app.Post("/multipart/complete", handleInternalMultipartCompleteFiber(om.TransferService))

	reqBody := internalapi.InternalMultipartCompleteRequest{
		UploadId: "non-existent",
		Parts:    []internalapi.InternalMultipartPart{},
	}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest("POST", "/multipart/complete", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, _ := app.Test(req)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404, got %d", resp.StatusCode)
	}
}

func TestHandleInternalMultipartCompletePreservesPartOrderAndOpaqueETags(t *testing.T) {
	const uploadID = "provider-upload-order-test"
	fake := &internalDRSStorageFake{}
	om := newInternalDRSObjectManager(&testutils.MockDatabase{}, fake)
	multipartUploadSessions.Store(uploadID, multipartSession{Bucket: "bucket-a", Key: "path/object.bin"})
	t.Cleanup(func() { multipartUploadSessions.Delete(uploadID) })

	body, _ := json.Marshal(internalapi.InternalMultipartCompleteRequest{
		UploadId: uploadID,
		Parts: []internalapi.InternalMultipartPart{
			{PartNumber: 7, ETag: `"opaque-seven"`},
			{PartNumber: 2, ETag: `"opaque-two"`},
		},
	})
	app := fiber.New()
	app.Post("/multipart/complete", handleInternalMultipartCompleteFiber(om.TransferService))
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/multipart/complete", bytes.NewBuffer(body)))
	if err != nil {
		t.Fatalf("complete request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if len(fake.completeParts) != 2 || fake.completeParts[0].PartNumber != 7 || fake.completeParts[0].ETag != `"opaque-seven"` || fake.completeParts[1].PartNumber != 2 || fake.completeParts[1].ETag != `"opaque-two"` {
		t.Fatalf("completed parts were not preserved in caller order: %+v", fake.completeParts)
	}
	if _, ok := multipartUploadSessions.Load(uploadID); ok {
		t.Fatal("expected multipart session to be removed after completion")
	}
}

func TestHandleInternalMultipartCompleteDeletesSessionBeforeProviderError(t *testing.T) {
	const uploadID = "provider-upload-error-test"
	fake := &internalDRSStorageFake{completeErr: errors.New("provider completion failed")}
	om := newInternalDRSObjectManager(&testutils.MockDatabase{}, fake)
	multipartUploadSessions.Store(uploadID, multipartSession{Bucket: "bucket-a", Key: "path/object.bin"})
	t.Cleanup(func() { multipartUploadSessions.Delete(uploadID) })

	body, _ := json.Marshal(internalapi.InternalMultipartCompleteRequest{UploadId: uploadID})
	app := fiber.New()
	app.Post("/multipart/complete", handleInternalMultipartCompleteFiber(om.TransferService))
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/multipart/complete", bytes.NewBuffer(body)))
	if err != nil {
		t.Fatalf("complete request failed: %v", err)
	}
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected provider failure to map to 500, got %d", resp.StatusCode)
	}
	if _, ok := multipartUploadSessions.Load(uploadID); ok {
		t.Fatal("expected multipart session to be removed before provider completion")
	}
}
