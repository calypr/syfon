package internaldrs

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
	"github.com/gorilla/mux"
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
			"obj-allow": {Id: "obj-allow", CreatedTime: now, UpdatedTime: now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
			"obj-deny":  {Id: "obj-deny", CreatedTime: now, UpdatedTime: now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h2"}}},
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
				UpdatedTime: now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "samehash"}},
			},
			"obj-md5": {
				Id:          "obj-md5",
				CreatedTime: now,
				UpdatedTime: now,
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
				UpdatedTime: now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "samehash"}},
			},
			"obj-md5": {
				Id:          "obj-md5",
				CreatedTime: now,
				UpdatedTime: now,
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
				"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: now},
				"obj-2": {Id: "obj-2", CreatedTime: now, UpdatedTime: now},
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
			"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
		},
	}

	router := mux.NewRouter()
	RegisterInternalIndexRoutes(router, mockDB)

	t.Run("collection alias /index/index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/index", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("detail alias /index/index/{id}", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/index/obj-1", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("bulk alias /index/index/bulk/hashes", func(t *testing.T) {
		reqBody := `{"hashes":["sha256:h1"]}`
		req := httptest.NewRequest(http.MethodPost, "/index/index/bulk/hashes", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}
