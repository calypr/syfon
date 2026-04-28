package migrate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

func TestSQLiteDumpRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "records.sqlite")
	dump, err := OpenSQLiteDump(path)
	if err != nil {
		t.Fatalf("OpenSQLiteDump: %v", err)
	}
	defer dump.Close()

	record := MigrationRecord{
		ID:          "dg.test/roundtrip",
		Size:        1,
		CreatedTime: time.Now().UTC(),
		Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "sha"}},
	}
	if err := dump.LoadBatch(context.Background(), []MigrationRecord{record}); err != nil {
		t.Fatalf("LoadBatch: %v", err)
	}
	count, err := dump.Count(context.Background())
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 record, got %d", count)
	}

	var got []MigrationRecord
	if err := dump.ReadBatches(context.Background(), 10, func(batch []MigrationRecord) error {
		got = append(got, batch...)
		return nil
	}); err != nil {
		t.Fatalf("ReadBatches: %v", err)
	}
	if len(got) != 1 || got[0].ID != record.ID {
		t.Fatalf("unexpected roundtrip records: %+v", got)
	}
}
