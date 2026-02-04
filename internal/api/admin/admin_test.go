package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
	"github.com/gorilla/mux"
)

func TestAdminCredentialsFlow(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &testutils.MockUrlManager{}
	router := mux.NewRouter()
	RegisterAdminRoutes(router, mockDB, mockUM)

	// 1. Put Credential
	var cred core.S3Credential = core.S3Credential{
		Bucket:    "admin-bucket",
		AccessKey: "key",
		SecretKey: "secret",
	}
	body, _ := json.Marshal(cred)
	req, _ := http.NewRequest(http.MethodPut, "/admin/credentials", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// 2. List Credentials
	req, _ = http.NewRequest(http.MethodGet, "/admin/credentials", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	// 3. Delete Credential
	req, _ = http.NewRequest(http.MethodDelete, "/admin/credentials/admin-bucket", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("expected 204, got %d", rr.Code)
	}
}

func TestAdminSignURL(t *testing.T) {
	mockDB := &testutils.MockDatabase{}
	mockUM := &testutils.MockUrlManager{}
	router := mux.NewRouter()
	RegisterAdminRoutes(router, mockDB, mockUM)

	reqBody := SignURLRequest{
		URL:    "s3://bucket/key",
		Method: "GET",
	}
	body, _ := json.Marshal(reqBody)
	req, _ := http.NewRequest(http.MethodPost, "/admin/sign_url", bytes.NewBuffer(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}

	var resp SignURLResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.SignedURL == "" {
		t.Error("expected signed_url in response")
	}
}
