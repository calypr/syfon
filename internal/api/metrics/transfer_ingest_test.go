package metrics

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	internalauth "github.com/calypr/syfon/internal/auth"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func ptr[T any](v T) *T { return &v }

func TestNormalizeTransferDirection(t *testing.T) {
	if got := normalizeTransferDirection("upload"); got != models.ProviderTransferDirectionUpload {
		t.Fatalf("expected upload, got %q", got)
	}
	if got := normalizeTransferDirection("something-else"); got != models.ProviderTransferDirectionDownload {
		t.Fatalf("expected default download, got %q", got)
	}
}

func TestTransferPayloadToModel(t *testing.T) {
	ctx := common.WithRequestID(context.Background(), "req-1")
	session := internalauth.NewSession("gen3")
	session.SetSubject("user-sub")
	ctx = internalauth.WithSession(ctx, session)

	payload := transferEventPayload{
		EventType:      models.TransferEventAccessIssued,
		Direction:      "download",
		EventTime:      time.Now().UTC().Format(time.RFC3339Nano),
		ObjectID:       "did-1",
		BytesRequested: 10,
	}
	got, err := transferPayloadToModel(ctx, payload)
	if err != nil {
		t.Fatalf("transferPayloadToModel error: %v", err)
	}
	if got.EventType != models.TransferEventAccessIssued || got.Direction != models.ProviderTransferDirectionDownload {
		t.Fatalf("unexpected mapped event: %+v", got)
	}
	if got.RequestID != "req-1" {
		t.Fatalf("expected request id fallback, got %q", got.RequestID)
	}
	if got.EventID == "" || got.AccessGrantID == "" {
		t.Fatalf("expected generated ids, got event=%q grant=%q", got.EventID, got.AccessGrantID)
	}

	payload.BytesRequested = -1
	if _, err := transferPayloadToModel(ctx, payload); err == nil {
		t.Fatalf("expected negative-bytes validation error")
	}
}

func TestCheckProviderMetricsIngestAuth(t *testing.T) {
	ctxNonGen3 := metricsTestContext(context.Background(), "local", true, true, nil)
	body := &metricsapi.RecordProviderTransferEventsJSONRequestBody{
		Events: []metricsapi.ProviderTransferEvent{{
			Organization: ptr("org-a"),
			Project:      ptr("proj-a"),
		}},
	}
	if status, ok := checkProviderMetricsIngestAuth(ctxNonGen3, body); !ok || status != 0 {
		t.Fatalf("expected non-gen3 auth allowed, got status=%d ok=%v", status, ok)
	}

	ctxMissingHeader := metricsTestContext(context.Background(), "gen3", false, false, nil)
	if status, ok := checkProviderMetricsIngestAuth(ctxMissingHeader, body); ok || status != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized for missing header, got status=%d ok=%v", status, ok)
	}

	ctxForbidden := metricsTestContext(context.Background(), "gen3", true, true, map[string]map[string]bool{})
	if status, ok := checkProviderMetricsIngestAuth(ctxForbidden, body); ok || status != http.StatusForbidden {
		t.Fatalf("expected forbidden without ingest privilege, got status=%d ok=%v", status, ok)
	}

	ctxAllowed := metricsTestContext(context.Background(), "gen3", true, true, map[string]map[string]bool{
		"/programs/org-a/projects/proj-a": {"create": true},
	})
	if status, ok := checkProviderMetricsIngestAuth(ctxAllowed, body); !ok || status != 0 {
		t.Fatalf("expected allowed with scoped create privilege, got status=%d ok=%v", status, ok)
	}

	ctxOrgAllowed := metricsTestContext(context.Background(), "gen3", true, true, map[string]map[string]bool{
		"/programs/org-a": {"update": true},
	})
	orgBody := &metricsapi.RecordProviderTransferEventsJSONRequestBody{
		Events: []metricsapi.ProviderTransferEvent{{
			Organization: ptr("org-a"),
		}},
	}
	if status, ok := checkProviderMetricsIngestAuth(ctxOrgAllowed, orgBody); !ok || status != 0 {
		t.Fatalf("expected allowed with org-scoped update privilege, got status=%d ok=%v", status, ok)
	}
}

func TestRecordProviderTransferEventsAuthResponse(t *testing.T) {
	if _, ok := recordProviderTransferEventsAuthResponse(http.StatusUnauthorized).(metricsapi.RecordProviderTransferEvents401Response); !ok {
		t.Fatalf("expected 401 response object")
	}
	if _, ok := recordProviderTransferEventsAuthResponse(http.StatusForbidden).(metricsapi.RecordProviderTransferEvents403Response); !ok {
		t.Fatalf("expected 403 response object")
	}
	if _, ok := recordProviderTransferEventsAuthResponse(http.StatusBadRequest).(metricsapi.RecordProviderTransferEvents400Response); !ok {
		t.Fatalf("expected 400 response object")
	}
}
