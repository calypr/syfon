package checksumapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestSHA256ValidityRoute(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"abc": {
				Id: "abc",
				AccessMethods: &[]drs.AccessMethod{
					{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://bucket-a/key"}},
				},
			},
		},
		Credentials: map[string]core.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Region: "us-east-1"},
		},
	}
	app := fiber.New()
	RegisterCoreRoutes(app, db)

	body, _ := json.Marshal(map[string]any{"sha256": []string{"abc", "missing"}})
	req := httptest.NewRequest(http.MethodPost, "/index/v1/sha256/validity", bytes.NewReader(body))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	respBody, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(respBody))
	}
	var decoded map[string]bool
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if !decoded["abc"] || decoded["missing"] {
		t.Fatalf("unexpected response: %#v", decoded)
	}
}

func TestSHA256ValidityRouteValidation(t *testing.T) {
	db := &testutils.MockDatabase{}
	app := fiber.New()
	RegisterCoreRoutes(app, db)

	req := httptest.NewRequest(http.MethodPost, "/index/v1/sha256/validity", bytes.NewBufferString(`{"sha256":["   "]}`))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, string(body))
	}
}
