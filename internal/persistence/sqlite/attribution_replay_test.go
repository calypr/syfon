package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/usage"
)

func TestAccessIssuedBatchReplayKeepsEventAndGrantCounts(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	events := []usage.Event{
		{EventID: "issuance-1", EventType: usage.TransferEventAccessIssued, Direction: usage.ProviderTransferDirectionDownload, EventTime: now, ObjectID: "same-object", AccessID: "s3", Provider: "s3", Bucket: "bucket", StorageURL: "s3://bucket/object"},
		{EventID: "issuance-2", EventType: usage.TransferEventAccessIssued, Direction: usage.ProviderTransferDirectionDownload, EventTime: now.Add(time.Second), ObjectID: "same-object", AccessID: "s3", Provider: "s3", Bucket: "bucket", StorageURL: "s3://bucket/object"},
	}
	if err := db.RecordTransferAttributionEvents(ctx, events); err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if err := db.RecordTransferAttributionEvents(ctx, []usage.Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	var eventCount, grantCount, issueCount int
	if err := db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM transfer_attribution_event").Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(issue_count), 0) FROM access_grant").Scan(&grantCount, &issueCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 2 || grantCount != 1 || issueCount != 2 {
		t.Fatalf("after replay: events=%d grants=%d issue_count=%d, want 2,1,2", eventCount, grantCount, issueCount)
	}
}
