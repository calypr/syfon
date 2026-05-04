package postgres

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/calypr/syfon/apigen/server/drs"
	internalauth "github.com/calypr/syfon/internal/auth"
	"github.com/calypr/syfon/internal/common"
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

	mock.ExpectQuery(regexp.QuoteMeta("SELECT url, type FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-1").
		WillReturnRows(sqlmock.NewRows([]string{"url", "type"}).
			AddRow("s3://bucket/key-1", "s3").
			AddRow("s3://bucket/key-1", "s3").
			AddRow("gs://bucket/key-2", "gs"))

	mock.ExpectQuery(regexp.QuoteMeta("SELECT resource FROM drs_object_controlled_access WHERE object_id = $1 ORDER BY resource")).
		WithArgs("obj-1").
		WillReturnRows(sqlmock.NewRows([]string{"resource"}).
			AddRow("/programs/p1/projects/a").
			AddRow("/programs/p1/projects/b"))

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

func TestGetObject_IgnoresAuthContext(t *testing.T) {
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
	mock.ExpectQuery(regexp.QuoteMeta("SELECT url, type FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{"url", "type"}).
			AddRow("s3://bucket/key", "s3"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT resource FROM drs_object_controlled_access WHERE object_id = $1 ORDER BY resource")).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{"resource"}).AddRow("/programs/p1/projects/a"))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1")).
		WithArgs("obj-2").
		WillReturnRows(sqlmock.NewRows([]string{"type", "checksum"}))
	session := internalauth.NewSession("gen3")
	session.SetAuthorizations([]string{"/programs/p1/projects/other"}, nil, true)
	ctx := internalauth.WithSession(context.Background(), session)

	obj, err := pg.GetObject(ctx, "obj-2")
	if err != nil {
		t.Fatalf("expected object fetch to ignore auth context, got %v", err)
	}
	if obj.Id != "obj-2" {
		t.Fatalf("expected obj-2, got %+v", obj)
	}
}

func TestListObjectIDsByScopeOrgIncludesProjectScopes(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectQuery("ca\\.resource = \\$1 OR ca\\.resource LIKE \\$2").
		WithArgs("/organization/calypr", "/organization/calypr/project/%").
		WillReturnRows(sqlmock.NewRows([]string{"object_id"}).
			AddRow("org-wide").
			AddRow("project-scoped"))

	ids, err := pg.ListObjectIDsByScope(context.Background(), "calypr", "")
	if err != nil {
		t.Fatalf("ListObjectIDsByScope returned error: %v", err)
	}
	if len(ids) != 2 || ids[0] != "org-wide" || ids[1] != "project-scoped" {
		t.Fatalf("unexpected ids: %+v", ids)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresScopeResourceCondition(t *testing.T) {
	condition, args, err := postgresScopeResourceCondition("ca.resource", "org", "")
	if err != nil {
		t.Fatalf("postgresScopeResourceCondition returned error: %v", err)
	}
	if want := "(ca.resource = ? OR ca.resource LIKE ? ESCAPE '\\')"; condition != want {
		t.Fatalf("unexpected condition: got %q want %q", condition, want)
	}
	if len(args) != 2 || args[0] != "/organization/org" || args[1] != "/organization/org/project/%" {
		t.Fatalf("unexpected args: %+v", args)
	}

	condition, args, err = postgresScopeResourceCondition("ca.resource", "org", "project")
	if err != nil {
		t.Fatalf("postgresScopeResourceCondition returned error: %v", err)
	}
	if condition != "ca.resource = ?" {
		t.Fatalf("unexpected project condition: %q", condition)
	}
	if len(args) != 1 || args[0] != "/organization/org/project/project" {
		t.Fatalf("unexpected project args: %+v", args)
	}
}

func TestBulkDeleteObjects(t *testing.T) {
	t.Run("empty ids is noop", func(t *testing.T) {
		pg, _, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()
		if err := pg.BulkDeleteObjects(context.Background(), nil); err != nil {
			t.Fatalf("expected nil on empty ids, got %v", err)
		}
	})

	t.Run("deletes provided ids", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectBegin()
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object WHERE id = ANY($1)")).
			WithArgs(sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(0, 2))
		mock.ExpectCommit()

		if err := pg.BulkDeleteObjects(context.Background(), []string{"a", "b"}); err != nil {
			t.Fatalf("BulkDeleteObjects returned error: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestUpdateObjectAccessMethods(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)")).
		WithArgs("obj-1", "s3://bucket/key", drs.AccessMethodTypeS3).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := pg.UpdateObjectAccessMethods(context.Background(), "obj-1", []drs.AccessMethod{{
		Type: drs.AccessMethodTypeS3,
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: "s3://bucket/key"},
	}})
	if err != nil {
		t.Fatalf("UpdateObjectAccessMethods returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBulkUpdateAccessMethods(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()
	mock.MatchExpectationsInOrder(false)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)")).
		WithArgs("obj-1", "s3://bucket/key", drs.AccessMethodTypeS3).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM drs_object_access_method WHERE object_id = $1")).
		WithArgs("obj-2").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := pg.BulkUpdateAccessMethods(context.Background(), map[string][]drs.AccessMethod{
		"obj-1": {
			{
				Type: drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/key"},
			},
		},
		"obj-2": {
			{Type: drs.AccessMethodTypeS3}, // no URL, should be skipped after delete
		},
	})
	if err != nil {
		t.Fatalf("BulkUpdateAccessMethods returned error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

