package coreapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
	"github.com/gorilla/mux"
)

func TestSHA256ValidityRoute(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"abc": {
				Id: "abc",
				AccessMethods: []drs.AccessMethod{
					{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket-a/key"}},
				},
			},
		},
		Credentials: map[string]core.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Region: "us-east-1"},
		},
	}
	router := mux.NewRouter()
	RegisterCoreRoutes(router, db)

	body, _ := json.Marshal(map[string]any{"sha256": []string{"abc", "missing"}})
	req := httptest.NewRequest(http.MethodPost, "/index/internal/v1/sha256/validity", bytes.NewReader(body))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var resp map[string]bool
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !resp["abc"] || resp["missing"] {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestSHA256ValidityRouteValidation(t *testing.T) {
	db := &testutils.MockDatabase{}
	router := mux.NewRouter()
	RegisterCoreRoutes(router, db)

	req := httptest.NewRequest(http.MethodPost, "/index/internal/v1/sha256/validity", bytes.NewBufferString(`{"sha256":["   "]}`))
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}
