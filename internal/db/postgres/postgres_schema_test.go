package postgres

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSchemaEnsurers(t *testing.T) {
	t.Run("ensureObjectSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec("CREATE TABLE IF NOT EXISTS drs_object").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS drs_object_access_method").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS drs_object_checksum").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE TABLE IF NOT EXISTS drs_object_alias").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE drs_object_access_method ADD COLUMN IF NOT EXISTS org TEXT NOT NULL DEFAULT ''")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("ALTER TABLE drs_object_access_method ADD COLUMN IF NOT EXISTS project TEXT NOT NULL DEFAULT ''")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS drs_object_access_method_object_id_idx ON drs_object_access_method(object_id)")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS drs_object_checksum_object_id_idx ON drs_object_checksum(object_id)")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS drs_object_checksum_checksum_idx ON drs_object_checksum(checksum)")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS drs_object_access_method_scope_idx ON drs_object_access_method(org, project)")).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("CREATE INDEX IF NOT EXISTS drs_object_alias_object_id_idx ON drs_object_alias(object_id)")).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensureObjectSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ensureS3CredentialSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec(regexp.QuoteMeta(`
		ALTER TABLE s3_credential
		ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 's3'
	`)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE s3_credential ADD COLUMN IF NOT EXISTS billing_log_bucket TEXT`)).
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(`ALTER TABLE s3_credential ADD COLUMN IF NOT EXISTS billing_log_prefix TEXT`)).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensureS3CredentialSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ensureBucketScopeSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec(regexp.QuoteMeta(`CREATE TABLE IF NOT EXISTS bucket_scope (
			organization TEXT NOT NULL,
			project_id TEXT NOT NULL,
			bucket TEXT NOT NULL,
			path_prefix TEXT NULL,
			PRIMARY KEY (organization, project_id)
		)`)).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta(`CREATE INDEX IF NOT EXISTS idx_bucket_scope_bucket ON bucket_scope(bucket)`)).
			WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensureBucketScopeSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ensureLFSPendingSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec("CREATE TABLE IF NOT EXISTS lfs_pending_metadata").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_expires").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_created").WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensureLFSPendingSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ensureObjectUsageSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec("CREATE TABLE IF NOT EXISTS object_usage").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_object_usage_last_download_time").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_object_usage_last_upload_time").WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensureObjectUsageSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ensurePendingObjectUsageSchema", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec("CREATE TABLE IF NOT EXISTS object_usage_event").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_object_usage_event_object_id").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("CREATE INDEX IF NOT EXISTS idx_object_usage_event_event_time").WillReturnResult(sqlmock.NewResult(0, 0))

		if err := pg.ensurePendingObjectUsageSchema(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("GetServiceInfo", func(t *testing.T) {
		pg, _, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()
		got, err := pg.GetServiceInfo(context.Background())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got == nil || got.Id == "" || got.Name == "" {
			t.Fatalf("unexpected service info: %#v", got)
		}
	})
}
