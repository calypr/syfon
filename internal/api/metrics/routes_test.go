package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/testutils"
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
		req := httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/files?limit=10&offset=0&inactive_days=365", nil)
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
		req := httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/summary?inactive_days=365", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", rr.Code, rr.Body.String())
		}
		var resp core.FileUsageSummary
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.TotalFiles != 2 {
			t.Fatalf("expected total files 2, got %d", resp.TotalFiles)
		}
	})
}

func TestMetricsRoutes_GetNotFoundAndValidation(t *testing.T) {
	router := mux.NewRouter()
	RegisterMetricsRoutes(router, &testutils.MockDatabase{})

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/files/missing", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rr.Code, rr.Body.String())
	}

	req2 := httptest.NewRequest(http.MethodGet, "/internal/v1/metrics/files?limit=0", nil)
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", rr2.Code, rr2.Body.String())
	}
}
