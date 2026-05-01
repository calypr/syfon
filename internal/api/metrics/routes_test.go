package metrics

import (
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"

	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/metricsapi"

	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestMetricsRoutes_ListAndSummary(t *testing.T) {
	now := time.Now().UTC()
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"sha-1": {Id: "sha-1", Name: common.Ptr("f1"), Size: 1},
			"sha-2": {Id: "sha-2", Name: common.Ptr("f2"), Size: 2},
		},
		Usage: map[string]models.FileUsage{
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

	app := fiber.New()
	RegisterMetricsRoutes(app, db)

	t.Run("list", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?limit=10&offset=0&inactive_days=365", nil)
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp map[string]any
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, ok := resp["data"]; !ok {
			t.Fatalf("expected data field in response: %v", resp)
		}
	})

	t.Run("summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?inactive_days=365", nil)
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp metricsapi.FileUsageSummary
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.TotalFiles == nil || *resp.TotalFiles != 2 {
			t.Fatalf("expected total files 2, got %+v", resp.TotalFiles)
		}
	})
}

func TestMetricsRoutes_GetNotFoundAndValidation(t *testing.T) {
	app := fiber.New()
	RegisterMetricsRoutes(app, &testutils.MockDatabase{})

	req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files/missing", nil)
	httpResp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	body, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", httpResp.StatusCode, string(body))
	}

	req2 := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?limit=0", nil)
	httpResp2, err := app.Test(req2)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	body2, _ := io.ReadAll(httpResp2.Body)
	if httpResp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", httpResp2.StatusCode, string(body2))
	}
}

func TestMetricsRoutes_TransferAttribution(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"did-1": {
				Id:   "did-1",
				Size: 42,
			},
		},
		ObjectAuthz: map[string]map[string][]string{
			"did-1": {"calypr": {"proj-a"}},
		},
		TransferEvents: []models.TransferAttributionEvent{
			{
				EventID:        "grant-1",
				AccessGrantID:  "grant-1",
				EventType:      models.TransferEventAccessIssued,
				Direction:      models.ProviderTransferDirectionDownload,
				EventTime:      time.Date(2026, 4, 26, 19, 59, 0, 0, time.UTC),
				RequestID:      "request-1",
				ObjectID:       "did-1",
				SHA256:         "sha-1",
				ObjectSize:     42,
				Organization:   "calypr",
				Project:        "proj-a",
				AccessID:       "s3",
				Provider:       "s3",
				Bucket:         "bucket-a",
				StorageURL:     "s3://bucket-a/root/sha-1",
				BytesRequested: 42,
				ActorEmail:     "user@example.com",
				ActorSubject:   "user-sub",
				AuthMode:       "gen3",
			},
		},
	}
	app := fiber.New()
	RegisterMetricsRoutes(app, db)

	body := `{"events":[{
		"provider_event_id":"event-download-1",
		"access_grant_id":"grant-1",
		"direction":"download",
		"event_time":"2026-04-26T20:00:00Z",
		"request_id":"request-1",
		"provider_request_id":"provider-request-1",
		"object_id":"did-1",
		"sha256":"sha-1",
		"object_size":42,
		"organization":"calypr",
		"project":"proj-a",
		"access_id":"s3",
		"provider":"s3",
		"bucket":"bucket-a",
		"storage_url":"s3://bucket-a/root/sha-1",
		"range_start":0,
		"range_end":41,
		"bytes_transferred":42,
		"http_method":"GET",
		"http_status":200
	}]}`
	req := httptest.NewRequest(http.MethodPost, "/index/v1/metrics/provider-transfer-events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	httpResp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	respBody, _ := io.ReadAll(httpResp.Body)
	if httpResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", httpResp.StatusCode, string(respBody))
	}
	if len(db.ProviderTransferEvents) != 1 {
		t.Fatalf("expected one provider transfer event, got %+v", db.ProviderTransferEvents)
	}

	dupReq := httptest.NewRequest(http.MethodPost, "/index/v1/metrics/provider-transfer-events", strings.NewReader(body))
	dupReq.Header.Set("Content-Type", "application/json")
	dupResp, err := app.Test(dupReq)
	if err != nil {
		t.Fatalf("duplicate request failed: %v", err)
	}
	if dupResp.StatusCode != http.StatusCreated {
		dupBody, _ := io.ReadAll(dupResp.Body)
		t.Fatalf("expected duplicate insert to stay idempotent, got %d body=%s", dupResp.StatusCode, string(dupBody))
	}
	if len(db.ProviderTransferEvents) != 1 {
		t.Fatalf("duplicate event should not double insert, got %+v", db.ProviderTransferEvents)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/summary?organization=calypr&project=proj-a&direction=download&allow_stale=true", nil)
	summaryResp, err := app.Test(summaryReq)
	if err != nil {
		t.Fatalf("summary request failed: %v", err)
	}
	summaryBody, _ := io.ReadAll(summaryResp.Body)
	if summaryResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", summaryResp.StatusCode, string(summaryBody))
	}
	var summary models.TransferAttributionSummary
	if err := json.Unmarshal(summaryBody, &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.EventCount != 1 || summary.DownloadEventCount != 1 || summary.BytesDownloaded != 42 {
		t.Fatalf("unexpected summary: %+v", summary)
	}

	breakdownReq := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/breakdown?group_by=user&user=user@example.com&allow_stale=true", nil)
	breakdownResp, err := app.Test(breakdownReq)
	if err != nil {
		t.Fatalf("breakdown request failed: %v", err)
	}
	breakdownBody, _ := io.ReadAll(breakdownResp.Body)
	if breakdownResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", breakdownResp.StatusCode, string(breakdownBody))
	}
	var breakdown struct {
		GroupBy string                                `json:"group_by"`
		Data    []models.TransferAttributionBreakdown `json:"data"`
	}
	if err := json.Unmarshal(breakdownBody, &breakdown); err != nil {
		t.Fatalf("decode breakdown: %v", err)
	}
	if breakdown.GroupBy != "user" || len(breakdown.Data) != 1 || breakdown.Data[0].Key != "user@example.com" || breakdown.Data[0].BytesDownloaded != 42 {
		t.Fatalf("unexpected breakdown: %+v", breakdown)
	}
}

func TestMetricsRoutes_TransferAttributionAuthz(t *testing.T) {
	db := &testutils.MockDatabase{
		TransferEvents: []models.TransferAttributionEvent{
			{
				EventID:        "event-download-1",
				EventType:      models.TransferEventAccessIssued,
				Direction:      models.ProviderTransferDirectionDownload,
				EventTime:      time.Now().UTC(),
				ObjectID:       "did-1",
				SHA256:         "sha-1",
				Organization:   "calypr",
				Project:        "proj-a",
				Provider:       "s3",
				Bucket:         "bucket-a",
				BytesRequested: 42,
				ActorEmail:     "user@example.com",
				ActorSubject:   "user-sub",
			},
			{
				EventID:        "event-download-2",
				EventType:      models.TransferEventAccessIssued,
				Direction:      models.ProviderTransferDirectionDownload,
				EventTime:      time.Now().UTC(),
				ObjectID:       "did-2",
				SHA256:         "sha-2",
				Organization:   "calypr",
				Project:        "proj-b",
				Provider:       "s3",
				Bucket:         "bucket-a",
				BytesRequested: 99,
				ActorEmail:     "user@example.com",
				ActorSubject:   "user-sub",
			},
		},
	}
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if mode := c.Get("X-Test-Auth-Mode"); mode != "" {
			ctx := context.WithValue(c.Context(), common.AuthModeKey, mode)
			ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, c.Get("X-Test-Auth-Header") == "true")
			if privsJSON := c.Get("X-Test-Privileges"); privsJSON != "" {
				var privs map[string]map[string]bool
				if err := json.Unmarshal([]byte(privsJSON), &privs); err == nil {
					ctx = context.WithValue(ctx, common.UserPrivilegesKey, privs)
				}
			}
			c.SetContext(ctx)
		}
		return c.Next()
	})
	RegisterMetricsRoutes(app, db)

	projectPrivs, _ := json.Marshal(map[string]map[string]bool{
		"/programs/calypr/projects/proj-a": {"read": true},
	})
	globalPrivs, _ := json.Marshal(map[string]map[string]bool{
		"/programs": {"read": true},
	})

	t.Run("project reader can query user metrics inside project", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/breakdown?organization=calypr&project=proj-a&group_by=user&user=user@example.com&allow_stale=true", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(projectPrivs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp struct {
			Data []models.TransferAttributionBreakdown `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(resp.Data) != 1 || resp.Data[0].BytesDownloaded != 42 {
			t.Fatalf("expected only proj-a bytes, got %+v", resp.Data)
		}
	})

	t.Run("project reader cannot query another project", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/summary?organization=calypr&project=proj-b&user=user@example.com&allow_stale=true", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(projectPrivs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		if httpResp.StatusCode != http.StatusForbidden {
			body, _ := io.ReadAll(httpResp.Body)
			t.Fatalf("expected 403, got %d body=%s", httpResp.StatusCode, string(body))
		}
	})

	t.Run("project reader can query aggregate metrics for readable scopes", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/breakdown?group_by=user&user=user@example.com&allow_stale=true", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(projectPrivs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp struct {
			Data []models.TransferAttributionBreakdown `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if len(resp.Data) != 1 || resp.Data[0].BytesDownloaded != 42 {
			t.Fatalf("expected aggregate to include only readable scope bytes, got %+v", resp.Data)
		}
	})

	t.Run("global reader can query user globally", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/summary?user=user@example.com&allow_stale=true", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(globalPrivs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var summary models.TransferAttributionSummary
		if err := json.Unmarshal(body, &summary); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if summary.BytesDownloaded != 141 {
			t.Fatalf("expected global user bytes 141, got %+v", summary)
		}
	})
}

func TestMetricsRoutes_NoLegacyDownloadAttributionRoutes(t *testing.T) {
	app := fiber.New()
	RegisterMetricsRoutes(app, &testutils.MockDatabase{})

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/index/v1/metrics/download-events"},
		{method: http.MethodPost, path: "/index/v1/metrics/transfer-events"},
		{method: http.MethodGet, path: "/index/v1/metrics/downloads/summary"},
		{method: http.MethodGet, path: "/index/v1/metrics/downloads/breakdown"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("%s %s failed: %v", tc.method, tc.path, err)
		}
		if httpResp.StatusCode != http.StatusNotFound {
			body, _ := io.ReadAll(httpResp.Body)
			t.Fatalf("expected %s %s to be gone with 404, got %d body=%s", tc.method, tc.path, httpResp.StatusCode, string(body))
		}
	}
}

func TestMetricsSummaryAuthzAndScope(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"scoped-1": {Id: "scoped-1", Name: common.Ptr("f1"), Size: 1},
			"other-1":  {Id: "other-1", Name: common.Ptr("f2"), Size: 2},
		},
		ObjectAuthz: map[string]map[string][]string{
			"scoped-1": {"cbds": {"end_to_end_test"}},
			"other-1":  {"other": {"other"}},
		},
		Usage: map[string]models.FileUsage{
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
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		// Mock auth values from headers for testing
		if mode := c.Get("X-Test-Auth-Mode"); mode != "" {
			ctx := context.WithValue(c.Context(), common.AuthModeKey, mode)
			if c.Get("X-Test-Auth-Header") == "true" {
				ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
			} else if c.Get("X-Test-Auth-Header") == "false" {
				ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, false)
			}
			if privsJSON := c.Get("X-Test-Privileges"); privsJSON != "" {
				var privs map[string]map[string]bool
				if err := json.Unmarshal([]byte(privsJSON), &privs); err == nil {
					ctx = context.WithValue(ctx, common.UserPrivilegesKey, privs)
				}
			}
			c.SetContext(ctx)
		}
		return c.Next()
	})
	RegisterMetricsRoutes(app, db)

	t.Run("scope reader can access scoped summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?organization=cbds&project=end_to_end_test", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		privs, _ := json.Marshal(map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp metricsapi.FileUsageSummary
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.TotalFiles == nil || resp.TotalUploads == nil || resp.TotalDownloads == nil || *resp.TotalFiles != 1 || *resp.TotalUploads != 2 || *resp.TotalDownloads != 3 {
			t.Fatalf("unexpected scoped summary: %+v", resp)
		}
	})

	t.Run("missing auth header returns 401", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary?organization=cbds&project=end_to_end_test", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "false")
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d body=%s", httpResp.StatusCode, string(body))
		}
	})

	t.Run("program reader can access global summary via /programs read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/summary", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		privs, _ := json.Marshal(map[string]map[string]bool{
			"/programs": {"read": true},
		})
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
	})
}

func TestMetricsFilesAuthzAndScope(t *testing.T) {
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"scoped-1": {Id: "scoped-1", Name: common.Ptr("f1"), Size: 1},
			"other-1":  {Id: "other-1", Name: common.Ptr("f2"), Size: 2},
		},
		ObjectAuthz: map[string]map[string][]string{
			"scoped-1": {"cbds": {"end_to_end_test"}},
			"other-1":  {"other": {"other"}},
		},
		Usage: map[string]models.FileUsage{
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
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if mode := c.Get("X-Test-Auth-Mode"); mode != "" {
			ctx := context.WithValue(c.Context(), common.AuthModeKey, mode)
			if c.Get("X-Test-Auth-Header") == "true" {
				ctx = context.WithValue(ctx, common.AuthHeaderPresentKey, true)
			}
			if privsJSON := c.Get("X-Test-Privileges"); privsJSON != "" {
				var privs map[string]map[string]bool
				if err := json.Unmarshal([]byte(privsJSON), &privs); err == nil {
					ctx = context.WithValue(ctx, common.UserPrivilegesKey, privs)
				}
			}
			c.SetContext(ctx)
		}
		return c.Next()
	})
	RegisterMetricsRoutes(app, db)

	t.Run("scoped list returns only scoped objects", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files?organization=cbds&project=end_to_end_test&limit=10&offset=0", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		privs, _ := json.Marshal(map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp map[string]any
		if err := json.Unmarshal(body, &resp); err != nil {
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
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		privs, _ := json.Marshal(map[string]map[string]bool{
			"/programs/cbds/projects/end_to_end_test": {"read": true},
		})
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", httpResp.StatusCode, string(body))
		}
	})

	t.Run("global object lookup allowed via /programs read", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/files/other-1", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		privs, _ := json.Marshal(map[string]map[string]bool{
			"/programs": {"read": true},
		})
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
	})
}
