package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/metricsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/testutils"
	"github.com/gorilla/mux"
)

func TestMetricsRoutes_ListAndSummary(t *testing.T) {
	now := time.Now().UTC()
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {Id: "sha-1", Name: "f1", Size: 1},
			"sha-2": {Id: "sha-2", Name: "f2", Size: 2},
		},
		Usage: map[string]core.FileUsage{
			"sha-1": {
				ObjectID:      "sha-1",
				Name:          "f1",
				Size:          1,
				UploadCount:   1,
				DownloadCount: 3,
				LastDownloadTime: func() *time.Time {
					t := now.AddDate(0, 0, -10)
					return &t
				}(),
			},
			"sha-2": {
				ObjectID:      "sha-2",
				Name:          "f2",
				Size:          2,
				UploadCount:   1,
				DownloadCount: 0,
			},
		},
	}

	router := mux.NewRouter()
	RegisterMetricsRoutes(router, db)

	t.Run("list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?limit=10&offset=0&inactive_days=365", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := resp["data"]; !ok {
			t.Fatalf("expected data field in response: %v", resp)
		}
	})

	t.Run("summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?inactive_days=365", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		var resp metricsapi.FileUsageSummary
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.GetTotalFiles() != 2 {
			t.Fatalf("expected total files 2, got %d", resp.GetTotalFiles())
		}
	})
}

func TestMetricsRoutes_GetNotFoundAndValidation(t *testing.T) {
	router := mux.NewRouter()
	RegisterMetricsRoutes(router, &testutils.MockDatabase{})

	req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files/missing", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?limit=0", nil)
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr2.Code, rr2.Body.String())
	}
}

func TestMetricsSummaryAuthzAndScope(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"scoped-1": {Id: "scoped-1", Name: "f1", Size: 1},
			"other-1":  {Id: "other-1", Name: "f2", Size: 2},
		},
		ObjectAuthz: map[string][]string{
			"scoped-1": {"/programs/cbds/projects/end_to_end_test"},
			"other-1":  {"/programs/other/projects/other"},
		},
		Usage: map[string]core.FileUsage{
			"scoped-1": {
				ObjectID:      "scoped-1",
				UploadCount:   2,
				DownloadCount: 3,
			},
			"other-1": {
				ObjectID:      "other-1",
				UploadCount:   7,
				DownloadCount: 11,
			},
		},
	}
	router := mux.NewRouter()
	RegisterMetricsRoutes(router, db)

	t.Run("scope reader can access scoped summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?organization=cbds&project=end_to_end_test", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		var resp metricsapi.FileUsageSummary
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.GetTotalFiles() != 1 || resp.GetTotalUploads() != 2 || resp.GetTotalDownloads() != 3 {
			t.Fatalf("unexpected scoped summary: %+v", resp)
		}
	})

	t.Run("missing auth header returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?organization=cbds&project=end_to_end_test", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, false)
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("program reader can access global summary via /programs read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs": {"read": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}

func TestMetricsFilesAuthzAndScope(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"scoped-1": {Id: "scoped-1", Name: "f1", Size: 1},
			"other-1":  {Id: "other-1", Name: "f2", Size: 2},
		},
		ObjectAuthz: map[string][]string{
			"scoped-1": {"/programs/cbds/projects/end_to_end_test"},
			"other-1":  {"/programs/other/projects/other"},
		},
		Usage: map[string]core.FileUsage{
			"scoped-1": {
				ObjectID:      "scoped-1",
				Name:          "f1",
				Size:          1,
				UploadCount:   2,
				DownloadCount: 3,
			},
			"other-1": {
				ObjectID:      "other-1",
				Name:          "f2",
				Size:          2,
				UploadCount:   7,
				DownloadCount: 11,
			},
		},
	}
	router := mux.NewRouter()
	RegisterMetricsRoutes(router, db)

	t.Run("scoped list returns only scoped objects", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?organization=cbds&project=end_to_end_test&limit=10&offset=0", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		var resp map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		data, ok := resp["data"].([]any)
		if !ok {
			t.Fatalf("missing data field: %v", resp)
		}
		if len(data) != 1 {
			t.Fatalf("expected 1 scoped item, got %d payload=%v", len(data), resp)
		}
		first, ok := data[0].(map[string]any)
		if !ok {
			t.Fatalf("unexpected data item type: %T", data[0])
		}
		if first["object_id"] != "scoped-1" {
			t.Fatalf("expected scoped-1, got %v", first["object_id"])
		}
	})

	t.Run("scoped object lookup outside scope returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files/other-1?organization=cbds&project=end_to_end_test", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
		}
	})

	t.Run("global object lookup allowed via /programs read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files/other-1", nil)
		ctx := context.WithValue(req.Context(), core.AuthModeKey, "gen3")
		ctx = context.WithValue(ctx, core.AuthHeaderPresentKey, true)
		ctx = context.WithValue(ctx, core.UserPrivilegesKey, map[string]map[string]bool{
			"/programs": {"read": true},
		})
		req = req.WithContext(ctx)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
	})
}
