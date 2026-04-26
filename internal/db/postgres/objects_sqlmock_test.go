package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/calypr/syfon/internal/common"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDeleteObject(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT object_id FROM drs_object_alias WHERE alias_id = $1")).
			WithArgs("obj-1").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object WHERE id = $1")).
			WithArgs("obj-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		if err := pg.DeleteObject(context.Background(), "obj-1"); err != nil {
			t.Fatalf("DeleteObject returned error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT object_id FROM drs_object_alias WHERE alias_id = $1")).
			WithArgs("missing").
			WillReturnError(sql.ErrNoRows)
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object WHERE id = $1")).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectRollback()

		err := pg.DeleteObject(context.Background(), "missing")
		if !errors.Is(err, common.ErrNotFound) {
			t.Fatalf("expected not found error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("alias", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT object_id FROM drs_object_alias WHERE alias_id = $1")).
			WithArgs("alias-1").
			WillReturnRows(sqlmock.NewRows([]string{"object_id"}).AddRow("obj-1"))
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object WHERE id = $1")).
			WithArgs("obj-1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()

		if err := pg.DeleteObject(context.Background(), "alias-1"); err != nil {
			t.Fatalf("DeleteObject returned error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestGetObject_NotFound(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`)).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT object_id FROM drs_object_alias WHERE alias_id = $1")).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := pg.GetObject(context.Background(), "missing")
	if !errors.Is(err, common.ErrNotFound) {
		t.Fatalf("expected not found error, got %v", err)
	}
}

func TestGetObject_DeduplicatesAndPropagatesAuthz(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	created := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	updated := created.Add(2 * time.Hour)
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`)).
		WithArgs("obj-1").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "size", "created_time", "updated_time", "name", "version", "description",
		}).AddRow("obj-1", int64(123), created, updated, "file.txt", "v1", "desc"))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT url, type, org, project FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-1").
		WillReturnRows(sqlmock.NewRows([]string{"url", "type", "org", "project"}).
			AddRow("s3://bucket/key-1", "s3", "p1", "a").
			AddRow("s3://bucket/key-1", "s3", "p1", "a").
			AddRow("gs://bucket/key-2", "gs", "p1", "b"))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1")).
		WithArgs("obj-1").
		WillReturnRows(sqlmock.NewRows([]string{"type", "checksum"}).
			AddRow("sha256", "abc").
			AddRow("sha256", "abc").
			AddRow("md5", "def"))

	obj, err := pg.GetObject(context.Background(), "obj-1")
	if err != nil {
		t.Fatalf("GetObject returned error: %v", err)
	}
	if obj.Id != "obj-1" || obj.SelfUri != "drs://obj-1" {
		t.Fatalf("unexpected object identity fields: %+v", obj)
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) != 2 {
		t.Fatalf("expected 2 deduplicated access methods, got %+v", obj.AccessMethods)
	}
	if len(obj.Checksums) != 2 {
		t.Fatalf("expected 2 deduplicated checksums, got %d", len(obj.Checksums))
	}
	if got := obj.Authorizations["p1"]; len(got) != 2 {
		t.Fatalf("expected 2 deduplicated authz projects, got %+v", obj.Authorizations)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetObject_Gen3Unauthorized(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	now := time.Date(2026, time.March, 1, 10, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`)).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "size", "created_time", "updated_time", "name", "version", "description",
		}).AddRow("obj-2", int64(1), now, now, "n", "v", "d"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT url, type, org, project FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{"url", "type", "org", "project"}).
			AddRow("s3://bucket/key", "s3", "p1", "a"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1")).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{"type", "checksum"}))
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT COUNT(*) FROM drs_object o
		WHERE o.id = $1 AND (
			NOT EXISTS (SELECT 1 FROM drs_object_access_method a WHERE a.object_id = o.id AND a.org != '')
			OR EXISTS (SELECT 1 FROM drs_object_access_method a WHERE a.object_id = o.id
				AND ('/programs/' || a.org || CASE WHEN a.project != '' THEN '/projects/' || a.project ELSE '' END) = ANY($2))
		)`)).
		WithArgs("obj-2", sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	ctx := context.WithValue(context.Background(), common.AuthModeKey, "gen3")
	ctx = context.WithValue(ctx, common.UserAuthzKey, []string{"/programs/p1/projects/other"})

	_, err := pg.GetObject(ctx, "obj-2")
	if !errors.Is(err, common.ErrUnauthorized) {
		t.Fatalf("expected unauthorized error, got %v", err)
	}
}
