package postgres

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/lib/pq"
)

func TestRecordTransferAttributionEvents_PersistsGrantAndCommits(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	when := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	start := int64(10)
	event := models.TransferAttributionEvent{
		EventID:           "event-1",
		EventType:         models.TransferEventAccessIssued,
		Direction:         "upload",
		EventTime:         when,
		RequestID:         "request-1",
		ObjectID:          "object-1",
		SHA256:            "sha-1",
		ObjectSize:        42,
		Organization:      "org",
		Project:           "project",
		AccessID:          "access-1",
		Provider:          "s3",
		Bucket:            "bucket",
		StorageURL:        "s3://bucket/object-1",
		RangeStart:        &start,
		BytesRequested:    20,
		BytesCompleted:    20,
		ActorEmail:        "actor@example.com",
		ActorSubject:      "subject-1",
		AuthMode:          "session",
		ClientName:        "client",
		ClientVersion:     "1.0",
		TransferSessionID: "session-1",
	}
	grantID := accessGrantIDFromEvent(event)

	mock.ExpectBegin()
	mock.ExpectPrepare("INSERT INTO transfer_attribution_event").ExpectExec().
		WithArgs(
			event.EventID, grantID, event.EventType, event.Direction, when, event.RequestID, event.ObjectID, event.SHA256, event.ObjectSize,
			event.Organization, event.Project, event.AccessID, event.Provider, event.Bucket, event.StorageURL,
			start, nil, event.BytesRequested, event.BytesCompleted,
			event.ActorEmail, event.ActorSubject, event.AuthMode, event.ClientName, event.ClientVersion, event.TransferSessionID,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO access_grant").
		WithArgs(grantID, when, when, event.ObjectID, event.SHA256, event.ObjectSize,
			event.Organization, event.Project, event.AccessID, event.Provider, event.Bucket, event.StorageURL,
			event.ActorEmail, event.ActorSubject, event.AuthMode).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := pg.RecordTransferAttributionEvents(context.Background(), []models.TransferAttributionEvent{
		{},
		{EventID: "ignored", EventType: "transfer_started"},
		event,
	}); err != nil {
		t.Fatalf("RecordTransferAttributionEvents returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRecordProviderTransferEvents_NormalizesAndRecordsUnmatchedEvent(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	when := time.Date(2026, time.February, 3, 4, 5, 6, 0, time.UTC)
	event := models.ProviderTransferEvent{
		ProviderEventID:    "provider-event-1",
		Direction:          "GET",
		EventTime:          when,
		RequestID:          "request-1",
		ProviderRequestID:  "provider-request-1",
		ObjectID:           "object-1",
		SHA256:             "sha-1",
		ObjectSize:         42,
		Organization:       "org",
		Project:            "project",
		AccessID:           "access-1",
		Provider:           " s3 ",
		Bucket:             " bucket ",
		ObjectKey:          " /object-1 ",
		StorageURL:         " s3://bucket/object-1 ",
		BytesTransferred:   42,
		HTTPMethod:         "GET",
		HTTPStatus:         206,
		RequesterPrincipal: "principal",
		SourceIP:           "127.0.0.1",
		UserAgent:          "client",
		RawEventRef:        "raw-1",
		ActorEmail:         "actor@example.com",
		ActorSubject:       "subject-1",
		AuthMode:           "session",
	}

	mock.ExpectBegin()
	prepared := mock.ExpectPrepare("INSERT INTO provider_transfer_event")
	mock.ExpectQuery("SELECT access_grant_id, first_issued_at").
		WithArgs("s3", "bucket", when.Add(15*time.Minute), when.Add(-24*time.Hour), "s3://bucket/object-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"access_grant_id", "first_issued_at", "last_issued_at", "issue_count", "object_id", "sha256", "object_size",
			"organization", "project", "access_id", "provider", "bucket", "storage_url", "actor_email", "actor_subject", "auth_mode",
		}))
	prepared.ExpectExec().
		WithArgs(
			event.ProviderEventID, "", models.ProviderTransferDirectionDownload, when, event.RequestID, event.ProviderRequestID,
			event.ObjectID, event.SHA256, event.ObjectSize, event.Organization, event.Project, event.AccessID, "s3", "bucket",
			"object-1", "s3://bucket/object-1", nil, nil, event.BytesTransferred, event.HTTPMethod, event.HTTPStatus,
			event.RequesterPrincipal, event.SourceIP, event.UserAgent, event.RawEventRef, event.ActorEmail, event.ActorSubject, event.AuthMode,
			models.ProviderTransferUnmatched,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := pg.RecordProviderTransferEvents(context.Background(), []models.ProviderTransferEvent{event}); err != nil {
		t.Fatalf("RecordProviderTransferEvents returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetFileUsageFlushesEventsAndReturnsLatestAccess(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	uploaded := time.Date(2026, time.March, 1, 10, 0, 0, 0, time.UTC)
	downloaded := uploaded.Add(time.Hour)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)).
		WillReturnRows(sqlmock.NewRows([]string{"object_id"}).AddRow("object-1"))
	mock.ExpectExec("INSERT INTO object_usage").
		WithArgs(pq.Array([]string{"object-1"}), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM object_usage_event e").
		WithArgs(pq.Array([]string{"object-1"})).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
		WHERE o.id = $1
	`)).
		WithArgs("object-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "size", "upload_count", "download_count", "last_upload_time", "last_download_time",
		}).AddRow("object-1", "object.dat", int64(42), int64(2), int64(3), uploaded, downloaded))

	usage, err := pg.GetFileUsage(context.Background(), "object-1")
	if err != nil {
		t.Fatalf("GetFileUsage returned error: %v", err)
	}
	if usage.ObjectID != "object-1" || usage.Name != "object.dat" || usage.Size != 42 {
		t.Fatalf("unexpected object usage identity: %+v", usage)
	}
	if usage.UploadCount != 2 || usage.DownloadCount != 3 {
		t.Fatalf("unexpected object usage counts: %+v", usage)
	}
	if usage.LastAccessTime == nil || !usage.LastAccessTime.Equal(downloaded) {
		t.Fatalf("expected latest access %v, got %+v", downloaded, usage.LastAccessTime)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListBucketVisibilityRows_RestrictsAndHydratesRows(t *testing.T) {
	t.Run("empty restricted resources are a no-op", func(t *testing.T) {
		pg, _, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		rows, err := pg.ListBucketVisibilityRows(context.Background(), nil, false, true)
		if err != nil {
			t.Fatalf("ListBucketVisibilityRows returned error: %v", err)
		}
		if len(rows) != 0 {
			t.Fatalf("expected no rows, got %+v", rows)
		}
	})

	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()
	mock.ExpectQuery("SELECT DISTINCT am.url, am.type, COALESCE\\(ca.resource, ''\\)").
		WithArgs(pq.Array([]string{"/organization/org/project/project"}), true).
		WillReturnRows(sqlmock.NewRows([]string{"url", "type", "resource"}).
			AddRow("s3://bucket/object", "s3", "/organization/org/project/project").
			AddRow("gs://bucket/object", "gs", ""))

	rows, err := pg.ListBucketVisibilityRows(context.Background(), []string{"/organization/org/project/project"}, true, true)
	if err != nil {
		t.Fatalf("ListBucketVisibilityRows returned error: %v", err)
	}
	if len(rows) != 2 || rows[0].AccessURL != "s3://bucket/object" || rows[1].Resource != "" {
		t.Fatalf("unexpected visibility rows: %+v", rows)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeleteObjectAlias_PreservesTransactionAndNotFoundIdentity(t *testing.T) {
	t.Run("success commits", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended('syfon-content-write', 0))")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object_alias WHERE alias_id = $1")).
			WithArgs("alias-1").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		if err := pg.DeleteObjectAlias(context.Background(), "alias-1"); err != nil {
			t.Fatalf("DeleteObjectAlias returned error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("missing alias rolls back with not found", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended('syfon-content-write', 0))")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object_alias WHERE alias_id = $1")).
			WithArgs("missing").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := pg.DeleteObjectAlias(context.Background(), "missing")
		if !errors.Is(err, common.ErrNotFound) {
			t.Fatalf("expected not found error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestGetTransferAttributionReports_MapAggregatesAndBreakdown(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectQuery("SELECT COUNT\\(\\*\\),").
		WithArgs("org").
		WillReturnRows(sqlmock.NewRows([]string{
			"event_count", "access_issued_count", "download_event_count", "upload_event_count",
			"bytes_requested", "bytes_downloaded", "bytes_uploaded",
		}).AddRow(int64(5), int64(3), int64(4), int64(1), int64(100), int64(80), int64(20)))

	summary, err := pg.GetTransferAttributionSummary(context.Background(), models.TransferAttributionFilter{Organization: "org"})
	if err != nil {
		t.Fatalf("GetTransferAttributionSummary returned error: %v", err)
	}
	if summary.EventCount != 5 || summary.AccessIssuedCount != 3 || summary.BytesDownloaded != 80 || summary.BytesUploaded != 20 {
		t.Fatalf("unexpected transfer summary: %+v", summary)
	}

	when := time.Date(2026, time.April, 2, 3, 4, 5, 0, time.UTC)
	mock.ExpectQuery("SELECT provider").
		WithArgs("org").
		WillReturnRows(sqlmock.NewRows([]string{
			"key", "organization", "project", "provider", "bucket", "sha256", "actor_email", "actor_subject",
			"event_count", "bytes_requested", "bytes_downloaded", "bytes_uploaded", "last_transfer_time",
		}).AddRow("s3:bucket", "", "", "s3", "bucket", "", "", "", int64(2), int64(40), int64(30), int64(10), when))

	breakdown, err := pg.GetTransferAttributionBreakdown(context.Background(), models.TransferAttributionFilter{Organization: "org"}, "provider")
	if err != nil {
		t.Fatalf("GetTransferAttributionBreakdown returned error: %v", err)
	}
	if len(breakdown) != 1 || breakdown[0].Key != "s3:bucket" || breakdown[0].EventCount != 2 || breakdown[0].LastTransferTime == nil {
		t.Fatalf("unexpected transfer breakdown: %+v", breakdown)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestListFileUsageByObjectIDs_ReturnsCountsAndLatestTimes(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	uploaded := time.Date(2026, time.May, 1, 10, 0, 0, 0, time.UTC)
	downloaded := uploaded.Add(2 * time.Hour)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)).
		WillReturnRows(sqlmock.NewRows([]string{"object_id"}))
	mock.ExpectCommit()
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
		WHERE o.id = ANY($1)
		ORDER BY o.id
	`)).
		WithArgs(pq.Array([]string{"object-1", "object-2"})).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "size", "upload_count", "download_count", "last_upload_time", "last_download_time",
		}).
			AddRow("object-1", "one.dat", int64(10), int64(1), int64(0), uploaded, nil).
			AddRow("object-2", "two.dat", int64(20), int64(0), int64(2), nil, downloaded))

	usage, err := pg.ListFileUsageByObjectIDs(context.Background(), []string{"object-1", "object-2"})
	if err != nil {
		t.Fatalf("ListFileUsageByObjectIDs returned error: %v", err)
	}
	if len(usage) != 2 || usage[0].UploadCount != 1 || usage[1].DownloadCount != 2 {
		t.Fatalf("unexpected usage rows: %+v", usage)
	}
	if usage[0].LastAccessTime == nil || !usage[0].LastAccessTime.Equal(uploaded) || usage[1].LastAccessTime == nil || !usage[1].LastAccessTime.Equal(downloaded) {
		t.Fatalf("unexpected latest access times: %+v", usage)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateObjectAlias_ValidatesCanonicalAndAliasClaimsInTransaction(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("SELECT pg_advisory_xact_lock(hashtextextended('syfon-content-write', 0))")).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM drs_object WHERE id = $1")).
		WithArgs("object-1").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("object-1"))
	mock.ExpectQuery("SELECT DISTINCT replace\\(lower\\(trim\\(checksum\\)\\)").
		WithArgs("object-1").WillReturnRows(sqlmock.NewRows([]string{"checksum"}))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT resource FROM drs_object_controlled_access WHERE object_id = $1")).
		WithArgs("object-1").WillReturnRows(sqlmock.NewRows([]string{"resource"}))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT id FROM drs_object WHERE id = $1")).
		WithArgs("alias-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT object_id FROM drs_object_alias WHERE alias_id = $1")).
		WithArgs("alias-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO drs_object_alias").
		WithArgs("alias-1", "object-1").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	if err := pg.CreateObjectAlias(context.Background(), " alias-1 ", " object-1 "); err != nil {
		t.Fatalf("CreateObjectAlias returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
