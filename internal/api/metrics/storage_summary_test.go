package metrics

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/metricsapi"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/gofiber/fiber/v3"
)

func TestStorageMetricsRoutes_SummaryAndChildren(t *testing.T) {
	now := time.Now().UTC()
	db := &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"obj-1": {Id: "obj-1", Name: common.Ptr("data/a.txt"), Size: 10, UpdatedTime: &now},
			"obj-2": {Id: "obj-2", Name: common.Ptr("data/a.txt"), Size: 20, UpdatedTime: &now},
			"obj-3": {Id: "obj-3", Name: common.Ptr("data/nested/b.txt"), Size: 30, UpdatedTime: &now},
		},
		ObjectAuthz: map[string]map[string][]string{
			"obj-1": {"cbds": {"end_to_end_test"}},
			"obj-2": {"cbds": {"end_to_end_test"}},
			"obj-3": {"cbds": {"end_to_end_test"}},
		},
		Usage: map[string]models.FileUsage{
			"obj-1": {ObjectID: "obj-1", DownloadCount: 2, LastDownloadTime: timePtr(now.Add(-3 * time.Hour))},
			"obj-2": {ObjectID: "obj-2", DownloadCount: 4, LastDownloadTime: timePtr(now.Add(-time.Hour))},
			"obj-3": {ObjectID: "obj-3", DownloadCount: 1, LastDownloadTime: timePtr(now.Add(-2 * time.Hour))},
		},
	}
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		if mode := c.Get("X-Test-Auth-Mode"); mode != "" {
			var privs map[string]map[string]bool
			if privsJSON := c.Get("X-Test-Privileges"); privsJSON != "" {
				_ = json.Unmarshal([]byte(privsJSON), &privs)
			}
			ctx := metricsTestContext(c.Context(), mode, c.Get("X-Test-Auth-Header") == "true", c.Get("X-Test-Auth-Header") == "true", privs)
			c.SetContext(ctx)
		}
		return c.Next()
	})
	RegisterMetricsRoutes(app, db)

	privs, _ := json.Marshal(map[string]map[string]bool{
		"/programs/cbds/projects/end_to_end_test": {"read": true},
	})

	t.Run("summary", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/storage/summary?organization=cbds&project=end_to_end_test&path=data", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp metricsapi.StoragePathSummary
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.RecordCount == nil || *resp.RecordCount != 3 || resp.FileCount == nil || *resp.FileCount != 2 || resp.DuplicatePathCount == nil || *resp.DuplicatePathCount != 1 {
			t.Fatalf("unexpected storage summary: %+v", resp)
		}
		if resp.DownloadCount == nil || *resp.DownloadCount != 7 {
			t.Fatalf("unexpected download count: %+v", resp)
		}
		if resp.LastDownloadTime == nil || !resp.LastDownloadTime.Equal(now.Add(-time.Hour)) {
			t.Fatalf("unexpected last download time: %+v", resp.LastDownloadTime)
		}
	})

	t.Run("children", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/storage/children?organization=cbds&project=end_to_end_test&path=data&sort_by=bytes&sort_order=desc", nil)
		req.Header.Set("X-Test-Auth-Mode", "gen3")
		req.Header.Set("X-Test-Auth-Header", "true")
		req.Header.Set("X-Test-Privileges", string(privs))
		httpResp, err := app.Test(req)
		if err != nil {
			t.Fatalf("test request failed: %v", err)
		}
		body, _ := io.ReadAll(httpResp.Body)
		if httpResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d body=%s", httpResp.StatusCode, string(body))
		}
		var resp metricsapi.StoragePathChildrenResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if resp.Items == nil || len(*resp.Items) != 2 {
			t.Fatalf("unexpected children response: %+v", resp)
		}
		first := (*resp.Items)[0]
		if first.Name == nil || *first.Name != "nested" || first.Type == nil || *first.Type != metricsapi.Directory {
			t.Fatalf("unexpected first child: %+v", first)
		}
		if first.DownloadCount == nil || *first.DownloadCount != 1 {
			t.Fatalf("unexpected first child download count: %+v", first)
		}
	})
}

func TestStorageMetricsRoutes_Validation(t *testing.T) {
	app := fiber.New()
	RegisterMetricsRoutes(app, &testutils.MockDatabase{})

	req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/storage/children?organization=cbds&project=end_to_end_test&path=../bad", nil)
	httpResp, err := app.Test(req)
	if err != nil {
		t.Fatalf("test request failed: %v", err)
	}
	if httpResp.StatusCode != http.StatusBadRequest {
		body, _ := io.ReadAll(httpResp.Body)
		t.Fatalf("expected 400, got %d body=%s", httpResp.StatusCode, string(body))
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
