package postgres

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/syfon/internal/models"
)

func TestRecordFileUploadAndDownload(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectExec(regexp.MustCompile(`INSERT\s+INTO\s+object_usage_event\s*\(\s*object_id,\s*event_type,\s*event_time\s*\)\s+VALUES\s*\(\s*\$1,\s*'upload',\s*\$2\s*\)`).String()).
		WithArgs("obj-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := pg.RecordFileUpload(context.Background(), "obj-1"); err != nil {
		t.Fatalf("RecordFileUpload error: %v", err)
	}

	mock.ExpectExec(regexp.MustCompile(`INSERT\s+INTO\s+object_usage_event\s*\(\s*object_id,\s*event_type,\s*event_time\s*\)\s+VALUES\s*\(\s*\$1,\s*'download',\s*\$2\s*\)`).String()).
		WithArgs("obj-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := pg.RecordFileDownload(context.Background(), "obj-1"); err != nil {
		t.Fatalf("RecordFileDownload error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestTransferAttributionWhereHelpers(t *testing.T) {
	from := time.Now().UTC().Add(-time.Hour)
	to := time.Now().UTC()
	where, args := transferAttributionWhere(models.TransferAttributionFilter{
		Organization: "org",
		Project:      "proj",
		Direction:    "download",
		From:         &from,
		To:           &to,
		Provider:     "s3",
		Bucket:       "bucket-a",
		SHA256:       "sha-1",
		User:         "user@example.com",
	})
	if !strings.Contains(where, "organization = $1") || !strings.Contains(where, "(actor_email =") {
		t.Fatalf("unexpected transferAttributionWhere clause: %q", where)
	}
	if len(args) < 9 {
		t.Fatalf("expected populated args, got %d (%+v)", len(args), args)
	}

	whereRes, argsRes := transferAttributionWhereByResources(models.TransferAttributionFilter{}, []string{"/organization/org/project/proj"})
	if !strings.Contains(whereRes, "organization") {
		t.Fatalf("expected resource clause in whereByResources, got %q", whereRes)
	}
	if len(argsRes) == 0 {
		t.Fatalf("expected args for whereByResources")
	}

	whereNone, _ := transferAttributionWhereByResources(models.TransferAttributionFilter{}, nil)
	if !strings.Contains(whereNone, "1 = 0") {
		t.Fatalf("expected 1=0 guard for empty resources, got %q", whereNone)
	}
}

func TestProviderTransferHelpers(t *testing.T) {
	where, args := providerTransferWhere(models.TransferAttributionFilter{User: "alice", ReconciliationStatus: "all"})
	if !strings.Contains(where, "actor_email") {
		t.Fatalf("expected user filter in provider where clause: %q", where)
	}
	if len(args) < 2 {
		t.Fatalf("expected user args in provider where clause")
	}

	if key, _ := providerTransferGroupExpr("user"); !strings.Contains(key, "actor_email") {
		t.Fatalf("unexpected provider group expression for user: %q", key)
	}
	if key, _ := transferAttributionGroupExpr("provider"); !strings.Contains(key, "provider") {
		t.Fatalf("unexpected attribution group expression for provider: %q", key)
	}

	if got := normalizeTransferDirection("upload"); got != models.ProviderTransferDirectionUpload {
		t.Fatalf("expected upload direction to remain upload, got %q", got)
	}
	if got := normalizeTransferDirection("unknown"); got != models.ProviderTransferDirectionDownload {
		t.Fatalf("expected unknown => download, got %q", got)
	}
}

func TestRecordTransferAttributionEvents_EmptyInput(t *testing.T) {
	pg, _, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()
	if err := pg.RecordTransferAttributionEvents(context.Background(), nil); err != nil {
		t.Fatalf("expected nil for empty transfer attribution events, got %v", err)
	}
}

func TestRecordProviderTransferEvents_EmptyInput(t *testing.T) {
	pg, _, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()
	if err := pg.RecordProviderTransferEvents(context.Background(), nil); err != nil {
		t.Fatalf("expected nil for empty provider transfer events, got %v", err)
	}
}

func TestGetStoragePathSummary(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	now := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)).WillReturnRows(sqlmock.NewRows([]string{"object_id"}))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT bi.object_id, o.size, o.updated_time,
			COALESCE(u.download_count, 0), u.last_download_time,
			bi.normalized_path, bi.normalized_path
		FROM drs_object_browse_index bi
		JOIN drs_object o ON o.id = bi.object_id
		LEFT JOIN object_usage u ON u.object_id = bi.object_id
		WHERE bi.resource = $1
		ORDER BY bi.object_id
	`)).
		WithArgs("/organization/org/project/proj").
		WillReturnRows(sqlmock.NewRows([]string{"object_id", "size", "updated_time", "download_count", "last_download_time", "normalized_path", "normalized_path"}).
			AddRow("obj-1", int64(10), now.Add(-time.Hour), int64(2), now.Add(-3*time.Hour), "data/a.txt", "data/a.txt").
			AddRow("obj-2", int64(20), now, int64(4), now.Add(-30*time.Minute), "data/a.txt", "data/a.txt").
			AddRow("obj-3", int64(30), now.Add(-2*time.Hour), int64(1), now.Add(-90*time.Minute), "data/nested/b.txt", "data/nested/b.txt"))

	summary, err := pg.GetStoragePathSummary(context.Background(), "org", "proj", "data")
	if err != nil {
		t.Fatalf("GetStoragePathSummary returned error: %v", err)
	}
	if summary.RecordCount != 3 || summary.FileCount != 2 || summary.TotalBytes != 60 || summary.DuplicatePathCount != 1 || summary.DownloadCount != 7 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
	if summary.LastDownloadTime == nil || !summary.LastDownloadTime.Equal(now.Add(-30*time.Minute)) {
		t.Fatalf("unexpected last download time: %+v", summary.LastDownloadTime)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
