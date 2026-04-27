package postgres

import (
	"context"
	"database/sql"
	"errors"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/crypto"
	"github.com/calypr/syfon/internal/models"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func newMockPostgresDB(t *testing.T) (*PostgresDB, sqlmock.Sqlmock, *sql.DB) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	return &PostgresDB{db: db}, mock, db
}

func TestGetS3Credential(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	rows := sqlmock.NewRows([]string{"bucket", "provider", "region", "access_key", "secret_key", "endpoint", "billing_log_bucket", "billing_log_prefix"}).
		AddRow("b1", "s3", "us-east-1", "ak", "sk", "https://s3.example", "logs", "prefix")
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT bucket, provider, region, access_key, secret_key, endpoint,
		       COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '')
		FROM s3_credential WHERE bucket = $1`)).
		WithArgs("b1").
		WillReturnRows(rows)

	got, err := pg.GetS3Credential(context.Background(), "b1")
	if err != nil {
		t.Fatalf("GetS3Credential returned error: %v", err)
	}
	if got.Bucket != "b1" || got.Provider != "s3" {
		t.Fatalf("unexpected credential: %#v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestGetS3Credential_DecryptsEncryptedSecrets(t *testing.T) {
	t.Setenv(crypto.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	encAK, err := crypto.EncryptCredentialField("ak")
	if err != nil {
		t.Fatalf("encrypt access key: %v", err)
	}
	encSK, err := crypto.EncryptCredentialField("sk")
	if err != nil {
		t.Fatalf("encrypt secret key: %v", err)
	}

	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	rows := sqlmock.NewRows([]string{"bucket", "provider", "region", "access_key", "secret_key", "endpoint", "billing_log_bucket", "billing_log_prefix"}).
		AddRow("b1", "s3", "us-east-1", encAK, encSK, "https://s3.example", "", "")
	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT bucket, provider, region, access_key, secret_key, endpoint,
		       COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '')
		FROM s3_credential WHERE bucket = $1`)).
		WithArgs("b1").
		WillReturnRows(rows)

	got, err := pg.GetS3Credential(context.Background(), "b1")
	if err != nil {
		t.Fatalf("GetS3Credential returned error: %v", err)
	}
	if got.AccessKey != "ak" || got.SecretKey != "sk" {
		t.Fatalf("expected decrypted keys, got %+v", got)
	}
}

func TestGetS3CredentialNotFound(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT bucket, provider, region, access_key, secret_key, endpoint,
		       COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '')
		FROM s3_credential WHERE bucket = $1`)).
		WithArgs("missing").
		WillReturnError(sql.ErrNoRows)

	_, err := pg.GetS3Credential(context.Background(), "missing")
	if err == nil || err.Error() != "credential not found" {
		t.Fatalf("expected credential not found error, got %v", err)
	}
}

func TestSaveS3Credential(t *testing.T) {
	t.Setenv(crypto.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO s3_credential (bucket, provider, region, access_key, secret_key, endpoint, billing_log_bucket, billing_log_prefix)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (bucket) DO UPDATE SET
			provider = EXCLUDED.provider,
			region = EXCLUDED.region,
			access_key = EXCLUDED.access_key,
			secret_key = EXCLUDED.secret_key,
			endpoint = EXCLUDED.endpoint,
			billing_log_bucket = EXCLUDED.billing_log_bucket,
			billing_log_prefix = EXCLUDED.billing_log_prefix`)).
		WithArgs("b1", "s3", "us-east-1", sqlmock.AnyArg(), sqlmock.AnyArg(), "https://s3.example", "logs", "prefix").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := pg.SaveS3Credential(context.Background(), &models.S3Credential{
		Bucket:           "b1",
		Provider:         "",
		Region:           "us-east-1",
		AccessKey:        "ak",
		SecretKey:        "sk",
		Endpoint:         "https://s3.example",
		BillingLogBucket: "logs",
		BillingLogPrefix: "prefix",
	})
	if err != nil {
		t.Fatalf("SaveS3Credential returned error: %v", err)
	}
}

func TestDeleteS3Credential(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM bucket_scope WHERE bucket = $1")).
			WithArgs("b1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM s3_credential WHERE bucket = $1")).
			WithArgs("b1").
			WillReturnResult(sqlmock.NewResult(0, 1))

		if err := pg.DeleteS3Credential(context.Background(), "b1"); err != nil {
			t.Fatalf("DeleteS3Credential returned error: %v", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM bucket_scope WHERE bucket = $1")).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(regexp.QuoteMeta("DELETE FROM s3_credential WHERE bucket = $1")).
			WithArgs("missing").
			WillReturnResult(sqlmock.NewResult(0, 0))

		err := pg.DeleteS3Credential(context.Background(), "missing")
		if err == nil || err.Error() != "credential not found" {
			t.Fatalf("expected credential not found, got %v", err)
		}
	})
}

func TestListS3Credentials(t *testing.T) {
	pg, mock, rawDB := newMockPostgresDB(t)
	defer rawDB.Close()

	rows := sqlmock.NewRows([]string{"bucket", "provider", "region", "access_key", "secret_key", "endpoint", "billing_log_bucket", "billing_log_prefix"}).
		AddRow("b1", "s3", "us-east-1", "ak1", "sk1", "", "logs1", "prefix1").
		AddRow("b2", "gcs", "us-central1", "ak2", "sk2", "https://example", "logs2", "prefix2")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT bucket, provider, region, access_key, secret_key, endpoint, COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '') FROM s3_credential")).
		WillReturnRows(rows)

	got, err := pg.ListS3Credentials(context.Background())
	if err != nil {
		t.Fatalf("ListS3Credentials returned error: %v", err)
	}
	if len(got) != 2 || got[1].Bucket != "b2" {
		t.Fatalf("unexpected credentials: %#v", got)
	}
}

func TestCreateBucketScope(t *testing.T) {
	t.Run("validation", func(t *testing.T) {
		pg, _, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		if err := pg.CreateBucketScope(context.Background(), nil); err == nil {
			t.Fatal("expected nil scope validation error")
		}
		if err := pg.CreateBucketScope(context.Background(), &models.BucketScope{}); err == nil {
			t.Fatal("expected required field validation error")
		}
	})

	t.Run("idempotent existing scope", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		rows := sqlmock.NewRows([]string{"organization", "project_id", "bucket", "path_prefix"}).
			AddRow("org", "proj", "bucket-a", "prefix")
		mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
	`)).
			WithArgs("org", "proj").
			WillReturnRows(rows)

		err := pg.CreateBucketScope(context.Background(), &models.BucketScope{
			Organization: "org",
			ProjectID:    "proj",
			Bucket:       "bucket-a",
			PathPrefix:   "/prefix/",
		})
		if err != nil {
			t.Fatalf("expected idempotent success, got %v", err)
		}
	})

	t.Run("conflict", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		rows := sqlmock.NewRows([]string{"organization", "project_id", "bucket", "path_prefix"}).
			AddRow("org", "proj", "bucket-a", "prefix-a")
		mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
	`)).
			WithArgs("org", "proj").
			WillReturnRows(rows)

		err := pg.CreateBucketScope(context.Background(), &models.BucketScope{
			Organization: "org",
			ProjectID:    "proj",
			Bucket:       "bucket-b",
			PathPrefix:   "prefix-b",
		})
		if !errors.Is(err, common.ErrConflict) {
			t.Fatalf("expected conflict error, got %v", err)
		}
	})

	t.Run("create new", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
	`)).
			WithArgs("org", "proj").
			WillReturnError(sql.ErrNoRows)

		mock.ExpectExec(regexp.QuoteMeta(`
		INSERT INTO bucket_scope (organization, project_id, bucket, path_prefix)
		VALUES ($1, $2, $3, $4)
	`)).
			WithArgs("org", "proj", "bucket-a", "nested/path").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err := pg.CreateBucketScope(context.Background(), &models.BucketScope{
			Organization: " org ",
			ProjectID:    " proj ",
			Bucket:       " bucket-a ",
			PathPrefix:   "/nested/path/",
		})
		if err != nil {
			t.Fatalf("expected create success, got %v", err)
		}
	})
}

func TestGetAndListBucketScopes(t *testing.T) {
	t.Run("get not found", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
	`)).
			WithArgs("org", "proj").
			WillReturnError(sql.ErrNoRows)

		_, err := pg.GetBucketScope(context.Background(), "org", "proj")
		if !errors.Is(err, common.ErrNotFound) {
			t.Fatalf("expected not found error, got %v", err)
		}
	})

	t.Run("list", func(t *testing.T) {
		pg, mock, rawDB := newMockPostgresDB(t)
		defer rawDB.Close()

		rows := sqlmock.NewRows([]string{"organization", "project_id", "bucket", "path_prefix"}).
			AddRow("org1", "proj1", "b1", "").
			AddRow("org2", "proj2", "b2", "x")
		mock.ExpectQuery(regexp.QuoteMeta(`
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
	`)).WillReturnRows(rows)

		got, err := pg.ListBucketScopes(context.Background())
		if err != nil {
			t.Fatalf("ListBucketScopes returned error: %v", err)
		}
		if len(got) != 2 || got[1].Bucket != "b2" {
			t.Fatalf("unexpected scopes: %#v", got)
		}
	})
}
