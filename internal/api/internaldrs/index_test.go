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

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestParseScopeQuery(t *testing.T) {
	t.Run("organization and project", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?organization=prog&project=study", nil)
		org, project, ok, err := parseScopeQuery(req)
		if err != nil {
			t.Fatalf("parseScopeQuery returned error: %v", err)
		}
		if !ok || org != "prog" || project != "study" {
			t.Fatalf("unexpected scope parse result: ok=%v org=%q project=%q", ok, org, project)
		}
	})

	t.Run("project without organization fails", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/?project=study", nil)
		_, _, ok, err := parseScopeQuery(req)
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
		ObjectAuthz: map[string]map[string][]string{
			"obj-allow": {"org": {"p1"}},
			"obj-deny":  {"org": {"p2"}},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/?organization=org", nil)
	ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
	ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
		"/programs/org/projects/p1": {"read": true},
	})
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalList(rr, req, om)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Records []internalapi.InternalRecord `json:"records"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Records) != 1 {
		t.Fatalf("expected 1 visible record, got %d", len(payload.Records))
	}
	if payload.Records[0].Did != "obj-allow" {
		t.Fatalf("expected obj-allow, got %q", payload.Records[0].Did)
	}
}

func TestHandleInternalList_PaginatesIDs(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
			"obj-2": {Id: "obj-2", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h2"}}},
			"obj-3": {Id: "obj-3", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h3"}}},
		},
	}
	app := fiber.New()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	RegisterInternalIndexRoutes(app, om)

	req := httptest.NewRequest(http.MethodGet, "/index?limit=1&start=obj-1", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}
	var payload internalapi.ListRecordsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Records == nil || len(*payload.Records) != 1 {
		t.Fatalf("expected one paged record, got %+v", payload.Records)
	}
	if (*payload.Records)[0].Did != "obj-2" {
		t.Fatalf("expected obj-2 after start cursor, got %+v", (*payload.Records)[0])
	}
}

func TestHandleInternalList_PagePaginatesIDs(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h1"}}},
			"obj-2": {Id: "obj-2", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h2"}}},
			"obj-3": {Id: "obj-3", CreatedTime: now, UpdatedTime: &now, Checksums: []drs.Checksum{{Type: "sha256", Checksum: "h3"}}},
		},
	}
	app := fiber.New()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	RegisterInternalIndexRoutes(app, om)

	req := httptest.NewRequest(http.MethodGet, "/index?limit=1&page=1", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}
	var payload internalapi.ListRecordsResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Records == nil || len(*payload.Records) != 1 {
		t.Fatalf("expected one paged record, got %+v", payload.Records)
	}
	if (*payload.Records)[0].Did != "obj-2" {
		t.Fatalf("expected obj-2 on page 1 with zero-based offset, got %+v", (*payload.Records)[0])
	}
}

func TestPaginateInternalListIDsFiberDefaultLimit(t *testing.T) {
	ids := make([]string, defaultInternalListLimit+5)
	for i := range ids {
		ids[i] = "obj"
	}
	app := fiber.New()
	app.Get("/", func(c fiber.Ctx) error {
		paged, err := paginateInternalListIDsFiber(c, ids)
		if err != nil {
			return err
		}
		if len(paged) != defaultInternalListLimit {
			t.Fatalf("expected default limit %d, got %d", defaultInternalListLimit, len(paged))
		}
		return c.SendStatus(fiber.StatusOK)
	})
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
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
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalList(rr, req, om)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload internalapi.ListRecordsResponse
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Records == nil || len(*payload.Records) != 1 {
		t.Fatalf("expected 1 record, got %+v", payload.Records)
	}
	if got := (*payload.Records)[0].Did; got != "obj-sha" {
		t.Fatalf("expected obj-sha, got %q", got)
	}

	req = httptest.NewRequest(http.MethodGet, "/?hash=samehash&hash_type=md5", nil)
	rr = httptest.NewRecorder()
	om2 := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalList(rr, req, om2)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	payload = internalapi.ListRecordsResponse{}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.Records == nil || len(*payload.Records) != 1 {
		t.Fatalf("expected 1 record, got %+v", payload.Records)
	}
	if got := (*payload.Records)[0].Did; got != "obj-md5" {
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
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBulkHashes(om).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		Results map[string][]models.InternalObject `json:"results"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(payload.Results) != 1 {
		t.Fatalf("expected 1 result key, got %d", len(payload.Results))
	}
	objs := payload.Results["sha256:samehash"]
	if len(objs) != 1 {
		t.Fatalf("expected 1 record for hash, got %d", len(objs))
	}
	if objs[0].Id != "obj-sha" {
		t.Fatalf("expected obj-sha, got %q", objs[0].Id)
	}
}

func TestHandleInternalBulkSHA256Validity(t *testing.T) {
	now := time.Now().UTC()
	mockDB := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-sha": {
				Id:          "obj-sha",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "present"}},
			},
			"obj-md5": {
				Id:          "obj-md5",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "md5", Checksum: "md5-only"}},
			},
		},
	}

	req := httptest.NewRequest(http.MethodPost, "/index/bulk/sha256/validity", strings.NewReader(`{"sha256":["present","md5-only","missing"]}`))
	req.Header.Set("Content-Type", "application/json")
	app := fiber.New()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	app.Post("/index/bulk/sha256/validity", handleInternalBulkSHA256ValidityFiber(om))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
	}
	var payload map[string]bool
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !payload["present"] {
		t.Fatalf("expected present=true, got %+v", payload)
	}
	if payload["md5-only"] {
		t.Fatalf("expected md5-only=false, got %+v", payload)
	}
	if payload["missing"] {
		t.Fatalf("expected missing=false, got %+v", payload)
	}
}

func TestHandleInternalCreate_PersistsExplicitDidAndAuthz(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	reqBody := `{"records":[{"did":"obj-1","size":42,"auth":{"test":{"p1":["s3://bucket/path/obj-1"]}}}]}`
	req := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalCreate(rr, req, om)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	if got := mockDB.ObjectAuthz["obj-1"]; len(got["test"]) != 1 || got["test"][0] != "p1" {
		t.Fatalf("expected persisted authz, got %v", got)
	}
}

func TestHandleInternalCreate_RequiredFieldsFailAtDecode(t *testing.T) {
	t.Run("missing records", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
		reqBody := `{"size":42,"auth":{"test":{"p1":["s3://bucket/path/obj"]}}}`
		req := httptest.NewRequest(http.MethodPost, "/index", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()

		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalCreate(rr, req, om)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestHandleInternalBulkCreate_PersistsExplicitAuthz(t *testing.T) {
	mockDB := &testutils.MockDatabase{Objects: map[string]*drs.DrsObject{}}
	reqBody := `{"records":[{"did":"obj-bulk-1","size":7,"auth":{"test":{"p1":["s3://bucket/path/obj-bulk-1"]}}}]}`
	req := httptest.NewRequest(http.MethodPost, "/bulk/create", strings.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	handleInternalBulkCreate(om).ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	if got := mockDB.ObjectAuthz["obj-bulk-1"]; len(got["test"]) != 1 || got["test"][0] != "p1" {
		t.Fatalf("expected persisted authz, got %v", got)
	}
}

func TestHandleInternalDeleteByQuery(t *testing.T) {
	t.Run("requires scope query", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{}
		req := httptest.NewRequest(http.MethodDelete, "/", nil)
		rr := httptest.NewRecorder()

		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalDeleteByQuery(rr, req, om)

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rr.Code)
		}
	})

	t.Run("requires auth header in gen3 mode", func(t *testing.T) {
		mockDB := &testutils.MockDatabase{}
		req := httptest.NewRequest(http.MethodDelete, "/?organization=org", nil)
		ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()

		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalDeleteByQuery(rr, req, om)

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
			ObjectAuthz: map[string]map[string][]string{
				"obj-1": {"org": {"a"}},
				"obj-2": {"org": {"a"}},
			},
		}
		req := httptest.NewRequest(http.MethodDelete, "/?organization=org&project=a", nil)
		ctx := context.WithValue(req.Context(), common.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, common.UserPrivilegesKey, map[string]map[string]bool{
			"/programs/org/projects/a": {"delete": true},
		})
		req = req.WithContext(ctx)

		rr := httptest.NewRecorder()
		om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
		handleInternalDeleteByQuery(rr, req, om)

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
	om := core.NewObjectManager(mockDB, &testutils.MockUrlManager{})
	RegisterInternalIndexRoutes(app, om)

	t.Run("collection alias /index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index?organization=org", nil)
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

	t.Run("bulk alias /index/bulk/delete", func(t *testing.T) {
		reqBody := `{"hashes":["sha256:h1"]}`
		req := httptest.NewRequest(http.MethodPost, "/index/bulk/delete", strings.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("expected 200, got %d body=%s", resp.StatusCode, string(body))
		}
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), `"deleted":1`) {
			t.Fatalf("expected deleted count in response, got %s", string(body))
		}
	})
}
