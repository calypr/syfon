package metrics

import (
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"

	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
				EventID:       "grant-1",
				AccessGrantID: "grant-1",
				EventType:     models.TransferEventAccessIssued,
				EventTime:     time.Date(2026, 4, 26, 19, 59, 0, 0, time.UTC),
				RequestID:     "request-1",
				ObjectID:      "did-1",
				SHA256:        "sha-1",
				ObjectSize:    42,
				Organization:  "calypr",
				Project:       "proj-a",
				AccessID:      "s3",
				Provider:      "s3",
				Bucket:        "bucket-a",
				StorageURL:    "s3://bucket-a/root/sha-1",
				ActorEmail:    "user@example.com",
				ActorSubject:  "user-sub",
				AuthMode:      "gen3",
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

func TestMetricsRoutes_ProviderTransferSyncFreshnessMetadata(t *testing.T) {
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Provider: "s3"},
		},
		ProviderTransferEvents: []models.ProviderTransferEvent{
			{
				ProviderEventID:      "provider-event-1",
				Direction:            models.ProviderTransferDirectionDownload,
				EventTime:            time.Date(2026, 4, 26, 20, 0, 0, 0, time.UTC),
				ObjectID:             "did-1",
				SHA256:               "sha-1",
				Organization:         "calypr",
				Project:              "proj-a",
				Provider:             "s3",
				Bucket:               "bucket-a",
				BytesTransferred:     42,
				ReconciliationStatus: models.ProviderTransferMatched,
			},
		},
	}
	app := fiber.New()
	RegisterMetricsRoutes(app, db)

	query := "/index/v1/metrics/transfers/summary?organization=calypr&project=proj-a&provider=s3&bucket=bucket-a&from=2026-04-26T19:00:00Z&to=2026-04-26T21:00:00Z"
	staleReq := httptest.NewRequest(http.MethodGet, query, nil)
	staleResp, err := app.Test(staleReq)
	if err != nil {
		t.Fatalf("stale summary request failed: %v", err)
	}
	staleBody, _ := io.ReadAll(staleResp.Body)
	if staleResp.StatusCode != http.StatusOK {
		t.Fatalf("expected stale query to return 200 with freshness metadata, got %d body=%s", staleResp.StatusCode, string(staleBody))
	}
	var staleSummary metricsapi.TransferAttributionSummary
	if err := json.Unmarshal(staleBody, &staleSummary); err != nil {
		t.Fatalf("decode stale summary: %v", err)
	}
	if staleSummary.Freshness == nil || staleSummary.Freshness.IsStale == nil || !*staleSummary.Freshness.IsStale {
		t.Fatalf("expected stale response to carry freshness metadata, got %+v", staleSummary.Freshness)
	}
	if staleSummary.Freshness.MissingBuckets == nil || len(*staleSummary.Freshness.MissingBuckets) == 0 {
		t.Fatalf("expected stale response to include missing buckets, got %+v", staleSummary.Freshness)
	}

	syncBody := `{
		"provider":"s3",
		"bucket":"bucket-a",
		"organization":"calypr",
		"project":"proj-a",
		"from":"2026-04-26T19:00:00Z",
		"to":"2026-04-26T21:00:00Z",
		"status":"completed",
		"imported_events":1,
		"matched_events":1
	}`
	syncReq := httptest.NewRequest(http.MethodPost, "/index/v1/metrics/provider-transfer-sync", strings.NewReader(syncBody))
	syncReq.Header.Set("Content-Type", "application/json")
	syncResp, err := app.Test(syncReq)
	if err != nil {
		t.Fatalf("sync request failed: %v", err)
	}
	syncRespBody, _ := io.ReadAll(syncResp.Body)
	if syncResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected sync 201, got %d body=%s", syncResp.StatusCode, string(syncRespBody))
	}
	if len(db.ProviderSyncRuns) != 1 || db.ProviderSyncRuns[0].Status != models.ProviderTransferSyncCompleted {
		t.Fatalf("expected completed sync run, got %+v", db.ProviderSyncRuns)
	}
	listReq := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/provider-transfer-sync?provider=s3&bucket=bucket-a&from=2026-04-26T18:00:00Z&to=2026-04-26T22:00:00Z", nil)
	listResp, err := app.Test(listReq)
	if err != nil {
		t.Fatalf("list sync request failed: %v", err)
	}
	listBody, _ := io.ReadAll(listResp.Body)
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("expected sync status 200, got %d body=%s", listResp.StatusCode, string(listBody))
	}
	var syncList metricsapi.ProviderTransferSyncResponse
	if err := json.Unmarshal(listBody, &syncList); err != nil {
		t.Fatalf("decode sync list: %v", err)
	}
	if syncList.SyncRuns == nil || len(*syncList.SyncRuns) != 1 || (*syncList.SyncRuns)[0].Status == nil || *(*syncList.SyncRuns)[0].Status != metricsapi.Completed {
		t.Fatalf("expected listed completed sync run, got %+v", syncList.SyncRuns)
	}

	freshReq := httptest.NewRequest(http.MethodGet, query, nil)
	freshResp, err := app.Test(freshReq)
	if err != nil {
		t.Fatalf("fresh summary request failed: %v", err)
	}
	freshBody, _ := io.ReadAll(freshResp.Body)
	if freshResp.StatusCode != http.StatusOK {
		t.Fatalf("expected fresh query 200, got %d body=%s", freshResp.StatusCode, string(freshBody))
	}
	var summary metricsapi.TransferAttributionSummary
	if err := json.Unmarshal(freshBody, &summary); err != nil {
		t.Fatalf("decode fresh summary: %v", err)
	}
	if summary.Freshness == nil || summary.Freshness.IsStale == nil || *summary.Freshness.IsStale {
		t.Fatalf("expected non-stale freshness metadata, got %+v", summary.Freshness)
	}
	if summary.BytesDownloaded == nil || *summary.BytesDownloaded != 42 {
		t.Fatalf("expected provider bytes after sync, got %+v", summary)
	}
}

func TestMetricsRoutes_ProviderTransferSyncCollectsFileProviderEvents(t *testing.T) {
	root := t.TempDir()
	eventDir := filepath.Join(root, filepath.FromSlash(providerTransferEventPrefix))
	if err := os.MkdirAll(eventDir, 0o755); err != nil {
		t.Fatalf("create event dir: %v", err)
	}
	eventFile := filepath.Join(eventDir, "events.jsonl")
	eventJSON := `{
		"provider_event_id":"file-provider-event-1",
		"access_grant_id":"grant-file-1",
		"direction":"download",
		"event_time":"2026-04-26T20:00:00Z",
		"provider_request_id":"file-request-1",
		"object_key":"root/sha-file",
		"bytes_transferred":42,
		"http_method":"GET",
		"http_status":200
	}` + "\n"
	if err := os.WriteFile(eventFile, []byte(eventJSON), 0o644); err != nil {
		t.Fatalf("write provider event: %v", err)
	}

	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"bucket-a": {Bucket: "bucket-a", Provider: "file", Endpoint: root},
		},
		TransferEvents: []models.TransferAttributionEvent{
			{
				EventID:       "grant-file-1",
				AccessGrantID: "grant-file-1",
				EventType:     models.TransferEventAccessIssued,
				EventTime:     time.Date(2026, 4, 26, 19, 59, 0, 0, time.UTC),
				ObjectID:      "did-file",
				SHA256:        "sha-file",
				ObjectSize:    42,
				Organization:  "calypr",
				Project:       "proj-a",
				AccessID:      "file",
				Provider:      "file",
				Bucket:        "bucket-a",
				StorageURL:    filepath.ToSlash(filepath.Join(root, "root/sha-file")),
				ActorEmail:    "user@example.com",
				ActorSubject:  "user-sub",
				AuthMode:      "local",
			},
		},
	}
	app := fiber.New()
	RegisterMetricsRoutes(app, db)

	syncBody := `{
		"provider":"file",
		"bucket":"bucket-a",
		"organization":"calypr",
		"project":"proj-a",
		"from":"2026-04-26T19:00:00Z",
		"to":"2026-04-26T21:00:00Z"
	}`
	syncReq := httptest.NewRequest(http.MethodPost, "/index/v1/metrics/provider-transfer-sync", strings.NewReader(syncBody))
	syncReq.Header.Set("Content-Type", "application/json")
	syncResp, err := app.Test(syncReq)
	if err != nil {
		t.Fatalf("sync request failed: %v", err)
	}
	syncRespBody, _ := io.ReadAll(syncResp.Body)
	if syncResp.StatusCode != http.StatusCreated {
		t.Fatalf("expected sync 201, got %d body=%s", syncResp.StatusCode, string(syncRespBody))
	}
	if len(db.ProviderSyncRuns) != 1 || db.ProviderSyncRuns[0].Status != models.ProviderTransferSyncCompleted || db.ProviderSyncRuns[0].ImportedEvents != 1 || db.ProviderSyncRuns[0].MatchedEvents != 1 {
		t.Fatalf("expected completed sync run with imported event, got %+v", db.ProviderSyncRuns)
	}
	if len(db.ProviderTransferEvents) != 1 {
		t.Fatalf("expected provider event to be collected, got %+v", db.ProviderTransferEvents)
	}
	ev := db.ProviderTransferEvents[0]
	if ev.Provider != "file" || ev.Bucket != "bucket-a" || ev.Organization != "calypr" || ev.Project != "proj-a" || ev.SHA256 != "sha-file" {
		t.Fatalf("expected collected event to be reconciled against access grant, got %+v", ev)
	}

	summaryReq := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/summary?organization=calypr&project=proj-a&provider=file&bucket=bucket-a&from=2026-04-26T19:00:00Z&to=2026-04-26T21:00:00Z", nil)
	summaryResp, err := app.Test(summaryReq)
	if err != nil {
		t.Fatalf("summary request failed: %v", err)
	}
	summaryBody, _ := io.ReadAll(summaryResp.Body)
	if summaryResp.StatusCode != http.StatusOK {
		t.Fatalf("expected fresh summary 200, got %d body=%s", summaryResp.StatusCode, string(summaryBody))
	}
	var summary metricsapi.TransferAttributionSummary
	if err := json.Unmarshal(summaryBody, &summary); err != nil {
		t.Fatalf("decode summary: %v", err)
	}
	if summary.BytesDownloaded == nil || *summary.BytesDownloaded != 42 {
		t.Fatalf("expected collected bytes in summary, got %+v", summary)
	}
}

func TestMetricsRoutes_ProviderTransferSyncReportsEmptyImports(t *testing.T) {
	root := t.TempDir()
	db := &testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"bucket-empty": {Bucket: "bucket-empty", Provider: "file", Endpoint: root},
		},
	}
	app := fiber.New()
	RegisterMetricsRoutes(app, db)

	syncBody := `{
		"provider":"file",
		"bucket":"bucket-empty",
		"organization":"calypr",
		"project":"proj-a",
		"from":"2026-04-26T19:00:00Z",
		"to":"2026-04-26T21:00:00Z"
	}`
	req := httptest.NewRequest(http.MethodPost, "/index/v1/metrics/provider-transfer-sync", strings.NewReader(syncBody))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("sync request failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected sync 201, got %d body=%s", resp.StatusCode, string(body))
	}
	if len(db.ProviderSyncRuns) != 1 {
		t.Fatalf("expected one sync run, got %+v", db.ProviderSyncRuns)
	}
	run := db.ProviderSyncRuns[0]
	if run.Status != models.ProviderTransferSyncCompleted || run.ImportedEvents != 0 || !strings.Contains(run.ErrorMessage, "no billable transfer events") {
		t.Fatalf("expected completed sync with empty-import warning, got %+v", run)
	}
}

func TestProviderTransferLogParsers(t *testing.T) {
	t.Run("s3 server access log", func(t *testing.T) {
		line := `79a59df900b949e55d96a1e698f0ee2d my-data-bucket [26/Apr/2026:20:00:00 +0000] 192.0.2.10 arn:aws:iam::111122223333:user/alice REQ123 REST.GET.OBJECT root/sha-1 "GET /my-data-bucket/root/sha-1 HTTP/1.1" 200 - 42 42 12 11 "-" "git-drs/1.0" - HOSTID SigV4 ECDHE AuthHeader my-data-bucket.s3.amazonaws.com TLSv1.2 -`
		events, err := parseProviderTransferEventPayloads([]byte(line), "s3-log")
		if err != nil {
			t.Fatalf("parse s3 log: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one event, got %+v", events)
		}
		ev := events[0]
		if ev.Direction != models.ProviderTransferDirectionDownload || ev.Bucket != "my-data-bucket" || ev.ObjectKey != "root/sha-1" || ev.BytesTransferred != 42 || ev.ProviderRequestID != "REQ123" {
			t.Fatalf("unexpected s3 event: %+v", ev)
		}
	})

	t.Run("gcs audit json", func(t *testing.T) {
		raw := `{
			"insertId":"gcs-event-1",
			"timestamp":"2026-04-26T20:00:00Z",
			"protoPayload":{
				"methodName":"storage.objects.get",
				"resourceName":"projects/_/buckets/gcs-bucket/objects/root/sha-1",
				"authenticationInfo":{"principalEmail":"alice@example.com"}
			},
			"httpRequest":{"responseSize":"42","status":200}
		}`
		events, err := parseProviderTransferEventPayloads([]byte(raw), "gcs-log")
		if err != nil {
			t.Fatalf("parse gcs log: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one event, got %+v", events)
		}
		ev := events[0]
		if ev.Direction != models.ProviderTransferDirectionDownload || ev.Bucket != "gcs-bucket" || ev.ObjectKey != "root/sha-1" || ev.BytesTransferred != 42 || ev.ActorEmail != "alice@example.com" {
			t.Fatalf("unexpected gcs event: %+v", ev)
		}
	})

	t.Run("gcs exported log records array", func(t *testing.T) {
		raw := `{"entries":[{
			"insertId":"gcs-event-array-1",
			"timestamp":"2026-04-26T20:00:00Z",
			"protoPayload":{
				"methodName":"storage.objects.get",
				"resourceName":"projects/_/buckets/gcs-bucket/objects/root%2Fsha-2"
			},
			"httpRequest":{"responseSize":"84","status":206}
		}]}`
		events, err := parseProviderTransferEventPayloads([]byte(raw), "gcs-array-log")
		if err != nil {
			t.Fatalf("parse gcs array log: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one event, got %+v", events)
		}
		ev := events[0]
		if ev.Direction != models.ProviderTransferDirectionDownload || ev.Bucket != "gcs-bucket" || ev.ObjectKey != "root/sha-2" || ev.BytesTransferred != 84 {
			t.Fatalf("unexpected gcs array event: %+v", ev)
		}
	})

	t.Run("azure blob json", func(t *testing.T) {
		raw := `{
			"RequestIdHeader":"azure-request-1",
			"TimeGenerated":"2026-04-26T20:00:00Z",
			"OperationName":"GetBlob",
			"ContainerName":"az-container",
			"ObjectKey":"root/sha-1",
			"ResponseBodySize":42,
			"StatusCode":200,
			"CallerIpAddress":"192.0.2.20",
			"UserAgentHeader":"git-drs/1.0"
		}`
		events, err := parseProviderTransferEventPayloads([]byte(raw), "azure-log")
		if err != nil {
			t.Fatalf("parse azure log: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one event, got %+v", events)
		}
		ev := events[0]
		if ev.Direction != models.ProviderTransferDirectionDownload || ev.Bucket != "az-container" || ev.ObjectKey != "root/sha-1" || ev.BytesTransferred != 42 || ev.ProviderRequestID != "azure-request-1" {
			t.Fatalf("unexpected azure event: %+v", ev)
		}
	})

	t.Run("azure diagnostic records envelope", func(t *testing.T) {
		raw := `{"records":[{
			"RequestIdHeader":"azure-request-2",
			"TimeGenerated":"2026-04-26T20:01:00Z",
			"OperationName":"PutBlob",
			"ContainerName":"az-container",
			"ObjectKey":"root/uploaded-sha",
			"RequestBodySize":64,
			"StatusCode":201
		}]}`
		events, err := parseProviderTransferEventPayloads([]byte(raw), "azure-array-log")
		if err != nil {
			t.Fatalf("parse azure records log: %v", err)
		}
		if len(events) != 1 {
			t.Fatalf("expected one event, got %+v", events)
		}
		ev := events[0]
		if ev.Direction != models.ProviderTransferDirectionUpload || ev.Bucket != "az-container" || ev.ObjectKey != "root/uploaded-sha" || ev.BytesTransferred != 64 {
			t.Fatalf("unexpected azure records event: %+v", ev)
		}
	})
}

func TestProviderTransferLogFixtures(t *testing.T) {
	type wantEvent struct {
		direction string
		bucket    string
		key       string
		bytes     int64
		actor     string
	}
	cases := []struct {
		name string
		path string
		want []wantEvent
	}{
		{
			name: "s3 server access log",
			path: "testdata/provider_logs/s3_server_access.log",
			want: []wantEvent{
				{direction: models.ProviderTransferDirectionDownload, bucket: "syfon-data-bucket", key: "program-a/project-1/sha-download", bytes: 524288},
				{direction: models.ProviderTransferDirectionUpload, bucket: "syfon-data-bucket", key: "program-a/project-1/sha-upload", bytes: 7340032},
			},
		},
		{
			name: "gcs cloud logging export",
			path: "testdata/provider_logs/gcs_cloud_logging_export.json",
			want: []wantEvent{
				{direction: models.ProviderTransferDirectionDownload, bucket: "syfon-gcs-bucket", key: "program-a/project-1/sha-download", bytes: 524288, actor: "alice@example.com"},
				{direction: models.ProviderTransferDirectionUpload, bucket: "syfon-gcs-bucket", key: "program-a/project-1/sha-upload", bytes: 7340032, actor: "bob@example.com"},
			},
		},
		{
			name: "azure blob diagnostics",
			path: "testdata/provider_logs/azure_blob_diagnostics.json",
			want: []wantEvent{
				{direction: models.ProviderTransferDirectionDownload, bucket: "syfon-container", key: "program-a/project-1/sha-download", bytes: 524288, actor: "alice@example.com"},
				{direction: models.ProviderTransferDirectionUpload, bucket: "syfon-container", key: "program-a/project-1/sha-upload", bytes: 7340032, actor: "bob@example.com"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatalf("read fixture %s: %v", tc.path, err)
			}
			events, err := parseProviderTransferEventPayloads(data, tc.path)
			if err != nil {
				t.Fatalf("parse fixture %s: %v", tc.path, err)
			}
			if len(events) != len(tc.want) {
				t.Fatalf("expected %d events, got %d: %+v", len(tc.want), len(events), events)
			}
			for i, want := range tc.want {
				got := events[i]
				if got.Direction != want.direction || got.Bucket != want.bucket || got.ObjectKey != want.key || got.BytesTransferred != want.bytes {
					t.Fatalf("event %d mismatch: got %+v want %+v", i, got, want)
				}
				if want.actor != "" && got.ActorEmail != want.actor {
					t.Fatalf("event %d actor mismatch: got %q want %q", i, got.ActorEmail, want.actor)
				}
				if got.RawEventRef != tc.path {
					t.Fatalf("event %d raw ref mismatch: got %q want %q", i, got.RawEventRef, tc.path)
				}
			}
		})
	}
}

func TestMetricsRoutes_TransferAttributionAuthz(t *testing.T) {
	db := &testutils.MockDatabase{
		ProviderTransferEvents: []models.ProviderTransferEvent{
			{
				ProviderEventID:      "event-download-1",
				Direction:            models.ProviderTransferDirectionDownload,
				EventTime:            time.Now().UTC(),
				ObjectID:             "did-1",
				SHA256:               "sha-1",
				Organization:         "calypr",
				Project:              "proj-a",
				Provider:             "s3",
				Bucket:               "bucket-a",
				BytesTransferred:     42,
				ActorEmail:           "user@example.com",
				ActorSubject:         "user-sub",
				ReconciliationStatus: models.ProviderTransferMatched,
			},
			{
				ProviderEventID:      "event-download-2",
				Direction:            models.ProviderTransferDirectionDownload,
				EventTime:            time.Now().UTC(),
				ObjectID:             "did-2",
				SHA256:               "sha-2",
				Organization:         "calypr",
				Project:              "proj-b",
				Provider:             "s3",
				Bucket:               "bucket-a",
				BytesTransferred:     99,
				ActorEmail:           "user@example.com",
				ActorSubject:         "user-sub",
				ReconciliationStatus: models.ProviderTransferMatched,
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

	t.Run("project reader cannot query user globally", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/index/v1/metrics/transfers/breakdown?group_by=user&user=user@example.com&allow_stale=true", nil)
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
