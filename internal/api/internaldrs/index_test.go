package internaldrs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestParseScopeQuery(t *testing.T) {
	t.Run("authz takes precedence", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?authz=/programs/a&organization=ignored", nil)
		got, ok, err := parseScopeQuery(req)
		if err != nil {
			t.Fatalf("parseScopeQuery returned error: %v", err)
		}
		if !ok || got != "/programs/a" {
			t.Fatalf("unexpected scope parse result: ok=%v got=%q", ok, got)
		}
	})

	t.Run("organization and project", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?organization=prog&project=study", nil)
		got, ok, err := parseScopeQuery(req)
		if err != nil {
			t.Fatalf("parseScopeQuery returned error: %v", err)
		}
		if !ok || got != "/programs/prog/projects/study" {
			t.Fatalf("unexpected scope parse result: ok=%v got=%q", ok, got)
		}
	})

	t.Run("project without organization fails", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?project=study", nil)
		_, ok, err := parseScopeQuery(req)
		if err == nil {
			t.Fatal("expected validation error")
		}
		if ok {
			t.Fatal("expected hasScope=false on validation error")
		}
	})
}

func TestHandleInternalList_ScopeFilteringByReadPrivilege(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-allow": {Id: "obj-allow", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
			"obj-deny":  {Id: "obj-deny", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h2"}}},
		},
		ObjectAuthz: map[string][]string{
			"obj-allow": {"/programs/org/projects/p1"},
			"obj-deny":  {"/programs/org/projects/p2"},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/?organization=org", nil)
	ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/org/projects/p1": {"read": true},
	})
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handleInternalList(rr, req, mockDB)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("expected 1 visible record, got %d", len(payload.Records))
	}
	if got, _ := payload.Records[0]["did"].(string); got != "obj-allow" {
		t.Fatalf("expected obj-allow, got %q", got)
	}
}

func TestHandleInternalList_HashTypeFiltering(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-sha": {
				Id:          "obj-sha",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "samehash"}},
			},
			"obj-md5": {
				Id:          "obj-md5",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "md5", Checksum: "samehash"}},
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/?hash=sha256:samehash", nil)
	rr := httptest.NewRecorder()
	handleInternalList(rr, req, mockDB)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(payload.Records))
	}
	if got, _ := payload.Records[0]["did"].(string); got != "obj-sha" {
		t.Fatalf("expected obj-sha, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/?hash=samehash&hash_type=md5", nil)
	rr = httptest.NewRecorder()
	handleInternalList(rr, req, mockDB)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	payload = struct {
		Records []map[string]any `json:"records"`
	}{}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(payload.Records))
	}
	if got, _ := payload.Records[0]["did"].(string); got != "obj-md5" {
		t.Fatalf("expected obj-md5, got %q", got)
	}
}

func TestHandleInternalBulkHashes_HashTypeFiltering(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-sha": {
				Id:          "obj-sha",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "samehash"}},
			},
			"obj-md5": {
				Id:          "obj-md5",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "md5", Checksum: "samehash"}},
			},
		},
	}

	reqBody := `{"hashes":["sha256:samehash"]}`
	req := httptest.NewRequest(http.MethodPost, "/bulk/hashes", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handleInternalBulkHashes(mockDB).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Records []map[string]any `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(payload.Records))
	}
	if got, _ := payload.Records[0]["did"].(string); got != "obj-sha" {
		t.Fatalf("expected obj-sha, got %q", got)
	}
}

func TestHandleInternalCreate_PersistsExplicitDidAndAuthz(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	reqBody := `{"did":"obj-1","size":42,"authz":["/programs/test/projects/p1"]}`
	req := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handleInternalCreate(rr, req, mockDB)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Did    string            `json:"did"`
		Authz  []string          `json:"authz"`
		Hashes map[string]string `json:"hashes"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Did != "obj-1" {
		t.Fatalf("expected did obj-1, got %q", resp.Did)
	}
	if len(resp.Authz) != 1 || resp.Authz[0] != "/programs/test/projects/p1" {
		t.Fatalf("expected explicit authz to persist, got %v", resp.Authz)
	}
	if len(resp.Hashes) != 0 {
		t.Fatalf("expected no synthesized hashes, got %v", resp.Hashes)
	}
	if got := mockDB.ObjectAuthz[resp.Did]; len(got) != 1 || got[0] != "/programs/test/projects/p1" {
		t.Fatalf("expected persisted authz, got %v", got)
	}
}

func TestHandleInternalCreate_RequiredFieldsFailAtDecode(t *testing.T) {
	t.Run("missing did", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		reqBody := `{"size":42,"authz":["/programs/test/projects/p1"]}`
		req := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handleInternalCreate(rr, req, mockDB)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "Invalid request body") {
			t.Fatalf("expected decode validation error, got %s", rr.Body.String())
		}
	})

	t.Run("missing authz", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		reqBody := `{"did":"obj-1","size":42}`
		req := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		handleInternalCreate(rr, req, mockDB)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "Invalid request body") {
			t.Fatalf("expected decode validation error, got %s", rr.Body.String())
		}
	})
}

func TestHandleInternalBulkCreate_PersistsExplicitAuthz(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	reqBody := `{"records":[{"did":"obj-bulk-1","size":7,"authz":["/programs/test/projects/p1"]}]}`
	req := httptest.NewRequest(http.MethodPost, "/bulk/create", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	handleInternalBulkCreate(mockDB).ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Records []struct {
			Did   string   `json:"did"`
			Authz []string `json:"authz"`
		} `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Records[0].Did != "obj-bulk-1" {
		t.Fatalf("expected did obj-bulk-1, got %q", resp.Records[0].Did)
	}
	if len(resp.Records[0].Authz) != 1 || resp.Records[0].Authz[0] != "/programs/test/projects/p1" {
		t.Fatalf("expected explicit authz in bulk create response, got %v", resp.Records[0].Authz)
	}
	if got := mockDB.ObjectAuthz[resp.Records[0].Did]; len(got) != 1 || got[0] != "/programs/test/projects/p1" {
		t.Fatalf("expected persisted authz, got %v", got)
	}
}

func TestHandleInternalDeleteByQuery(t *testing.T) {
	t.Run("requires scope query", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{}
		req := httptest.NewRequest(http.MethodDelete, "/", nil)
		rr := httptest.NewRecorder()

		handleInternalDeleteByQuery(rr, req, mockDB)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rr.Code)
		}
	})

	t.Run("requires auth header in gen3 mode", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{}
		req := httptest.NewRequest(http.MethodDelete, "/?organization=org", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()

		handleInternalDeleteByQuery(rr, req, mockDB)

		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("deletes only authorized scoped records", func(t *testing.T) {
		now := time.Now().UTC()
		mockDB := &testutils.MockDatabase{
			Objects: map[string]*drs.DrsObject{
				"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: &now},
				"obj-2": {Id: "obj-2", CreatedTime: now, UpdatedTime: &now},
			},
			ObjectAuthz: map[string][]string{
				"obj-1": {"/programs/org/projects/a"},
				"obj-2": {"/programs/org/projects/a"},
			},
		}
		req := httptest.NewRequest(http.MethodDelete, "/?organization=org&project=a", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs/org/projects/a": {"delete": true},
		})
		req = req.WithContext(ctx)

		rr := httptest.NewRecorder()
		handleInternalDeleteByQuery(rr, req, mockDB)

		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		if _, ok := mockDB.Objects["obj-1"]; ok {
			t.Fatal("expected obj-1 to be deleted")
		}
		if _, ok := mockDB.Objects["obj-2"]; ok {
			t.Fatal("expected obj-2 to be deleted")
		}
		if !strings.Contains(rr.Body.String(), `"deleted":2`) {
			t.Fatalf("expected deleted count in response, got %s", rr.Body.String())
		}
	})
}

func TestRegisterInternalIndexRoutes_LegacyAliases(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
		},
	}

	app := fiber.New()
	RegisterInternalIndexRoutes(app, mockDB, &testutils.MockUrlManager{})

	t.Run("collection alias /index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
		}
	})

	t.Run("detail alias /index/{id}", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/obj-1", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
		}
	})

	t.Run("bulk alias /index/bulk/hashes", func(t *testing.T) {
		reqBody := `{"hashes":["sha256:h1"]}`
		req := httptest.NewRequest(http.MethodPost, "/index/bulk/hashes", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
		}
	})
}
