package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
)

func TestLFSUploadReceiptSurvivesStageAndPendingConsumption(t *testing.T) {
	for _, receiptFirst := range []bool{false, true} {
		name := "stage-before-upload"
		if receiptFirst {
			name = "upload-before-stage"
		}
		t.Run(name, func(t *testing.T) {
			db, err := NewSqliteDB(":memory:", nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })

			ctx := context.Background()
			oid := strings.Repeat("a", 64)
			now := time.Now().UTC().Truncate(time.Microsecond)
			receipt := transferlfs.UploadReceipt{
				OID: oid, Size: 5, SHA256: oid, StorageURL: "s3://bucket/" + oid,
				CompletedAt: now, ExpiresAt: now.Add(time.Hour),
			}
			stage := func(candidateName string) {
				t.Helper()
				entry := transferlfs.PendingMetadata{
					OID: oid, Candidate: lfsapi.DrsObjectCandidate{
						Name:      sqliteTestPtr(candidateName),
						Checksums: &[]lfsapi.Checksum{{Type: "sha256", Checksum: oid}},
					}, CreatedAt: now, ExpiresAt: now.Add(20 * time.Minute),
				}
				if err := db.SavePendingMetadata(ctx, []transferlfs.PendingMetadata{entry}); err != nil {
					t.Fatal(err)
				}
			}
			saveReceipt := func() {
				t.Helper()
				if err := db.SaveLFSUploadReceipt(ctx, receipt); err != nil {
					t.Fatal(err)
				}
			}
			if receiptFirst {
				saveReceipt()
				stage("first")
			} else {
				stage("first")
				saveReceipt()
			}
			stage("replacement")

			pending, err := db.GetPendingMetadata(ctx, oid)
			if err != nil {
				t.Fatal(err)
			}
			if sqliteTestStringVal(pending.Candidate.Name) != "replacement" || pending.UploadReceipt == nil || pending.UploadReceipt.StorageURL != receipt.StorageURL {
				t.Fatalf("pending metadata lost candidate or receipt: %+v", pending)
			}
			consumed, err := db.ConsumePendingMetadata(ctx, *pending)
			if err != nil || !consumed {
				t.Fatalf("consume pending metadata = %t, %v", consumed, err)
			}
			if _, err := db.GetPendingMetadata(ctx, oid); !errors.Is(err, errorapi.ErrNotFound) {
				t.Fatalf("consumed candidate remains: %v", err)
			}
			got, err := db.GetLFSUploadReceipt(ctx, oid)
			if err != nil || got.Size != receipt.Size || got.StorageURL != receipt.StorageURL {
				t.Fatalf("receipt after consume = %+v, %v", got, err)
			}
		})
	}
}

func TestExpiredLFSUploadReceiptIsUnavailable(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	oid := strings.Repeat("b", 64)
	completed := time.Now().UTC().Add(-2 * time.Hour)
	receipt := transferlfs.UploadReceipt{
		OID: oid, Size: 1, SHA256: oid, StorageURL: "s3://bucket/" + oid,
		CompletedAt: completed, ExpiresAt: completed.Add(time.Hour),
	}
	ctx := context.Background()
	if err := db.SaveLFSUploadReceipt(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetLFSUploadReceipt(ctx, oid); !errors.Is(err, errorapi.ErrNotFound) {
		t.Fatalf("expired receipt lookup = %v, want not found", err)
	}
}
