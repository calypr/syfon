package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	sqlitedb "github.com/calypr/syfon/internal/persistence/sqlite"
	"github.com/calypr/syfon/internal/usage"
)

func TestQueryTransferBreakdownPagesMoreThanOneThousandUsersAndObjects(t *testing.T) {
	db, err := sqlitedb.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatalf("open sqlite test store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const groupCount = 1001
	events := make([]usage.Event, 0, groupCount)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	for i := 0; i < groupCount; i++ {
		events = append(events, usage.Event{
			EventID:        fmt.Sprintf("breakdown-%04d", i),
			EventType:      usage.TransferEventAccessIssued,
			Direction:      usage.ProviderTransferDirectionDownload,
			EventTime:      now,
			ObjectID:       fmt.Sprintf("object-%04d", i),
			SHA256:         fmt.Sprintf("%064x", i+1),
			ActorEmail:     fmt.Sprintf("user-%04d@example.com", i),
			ObjectSize:     1,
			BytesRequested: 1,
		})
	}
	if err := db.RecordTransferAttributionEvents(context.Background(), events); err != nil {
		t.Fatalf("record transfer attribution events: %v", err)
	}

	for _, groupBy := range []string{"user", "object"} {
		t.Run(groupBy, func(t *testing.T) {
			var keys []string
			for offset := 0; ; offset += 1000 {
				rows, err := db.QueryTransferBreakdown(context.Background(), usage.Filter{}, groupBy, nil, 1000, offset)
				if err != nil {
					t.Fatalf("query transfer breakdown page at offset %d: %v", offset, err)
				}
				wantPageSize := 1000
				if offset == 1000 {
					wantPageSize = 1
				}
				if len(rows) != wantPageSize {
					t.Fatalf("page at offset %d returned %d groups, want %d", offset, len(rows), wantPageSize)
				}
				for _, row := range rows {
					if row.Key == nil {
						t.Fatal("breakdown returned a row without a group key")
					}
					keys = append(keys, *row.Key)
				}
				if len(rows) < 1000 {
					break
				}
			}
			if len(keys) != groupCount {
				t.Fatalf("collected %d groups, want all %d", len(keys), groupCount)
			}
			seen := make(map[string]struct{}, len(keys))
			for _, key := range keys {
				if _, exists := seen[key]; exists {
					t.Fatalf("group key %q appeared on multiple pages", key)
				}
				seen[key] = struct{}{}
			}
		})
	}
}
