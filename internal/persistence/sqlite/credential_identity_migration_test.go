package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestSqliteDBMigratesLegacyCredentialIdentityBeforeIndexesAndTriggers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy-credentials.db")
	legacy, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE s3_credential (
		bucket TEXT PRIMARY KEY,
		region TEXT,
		access_key TEXT,
		secret_key TEXT,
		endpoint TEXT
	)`); err != nil {
		t.Fatalf("create legacy credential table: %v", err)
	}
	if _, err := legacy.Exec(`INSERT INTO s3_credential (bucket, region, access_key, secret_key, endpoint)
		VALUES ('legacy-bucket', 'us-east-1', 'legacy-key', 'legacy-secret', 'https://s3.example')`); err != nil {
		t.Fatalf("insert legacy credential: %v", err)
	}
	if _, err := legacy.Exec(`CREATE TABLE bucket_scope (
		organization TEXT NOT NULL,
		project_id TEXT NOT NULL,
		bucket TEXT NOT NULL,
		path_prefix TEXT,
		PRIMARY KEY (organization, project_id)
	)`); err != nil {
		t.Fatalf("create legacy bucket scope table: %v", err)
	}
	if _, err := legacy.Exec(`INSERT INTO bucket_scope (organization, project_id, bucket, path_prefix)
		VALUES ('org', 'project', 'legacy-bucket', 'prefix')`); err != nil {
		t.Fatalf("insert legacy bucket scope: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	database, err := NewSqliteDB(dbPath, nil)
	if err != nil {
		t.Fatalf("open migrated database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	var credentialID, bucket, provider string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT credential_id, bucket, provider FROM s3_credential WHERE bucket = 'legacy-bucket'
	`).Scan(&credentialID, &bucket, &provider); err != nil {
		t.Fatalf("read migrated credential: %v", err)
	}
	if credentialID != "legacy-bucket" || bucket != "legacy-bucket" || provider != "s3" {
		t.Fatalf("migrated credential = (%q, %q, %q), want legacy-bucket identity and s3 provider", credentialID, bucket, provider)
	}

	var scopeCredentialID string
	if err := database.DB().QueryRowContext(context.Background(), `
		SELECT credential_id FROM bucket_scope WHERE organization = 'org' AND project_id = 'project'
	`).Scan(&scopeCredentialID); err != nil {
		t.Fatalf("read migrated bucket scope: %v", err)
	}
	if scopeCredentialID != "legacy-bucket" {
		t.Fatalf("migrated bucket scope credential ID = %q, want legacy-bucket", scopeCredentialID)
	}

	if _, err := database.DB().ExecContext(context.Background(), `
		INSERT INTO s3_credential (credential_id, bucket) VALUES ('second-credential', 'legacy-bucket')
	`); err == nil || !strings.Contains(err.Error(), "physical bucket is already configured under another credential") {
		t.Fatalf("migrated database duplicate bucket insert error = %v", err)
	}
	if _, err := database.DB().ExecContext(context.Background(), `
		INSERT INTO s3_credential (credential_id, bucket) VALUES ('second-credential', 'another-bucket')
	`); err != nil {
		t.Fatalf("insert second credential: %v", err)
	}
	if _, err := database.DB().ExecContext(context.Background(), `
		UPDATE s3_credential SET bucket = 'legacy-bucket' WHERE credential_id = 'second-credential'
	`); err == nil || !strings.Contains(err.Error(), "physical bucket is already configured under another credential") {
		t.Fatalf("migrated database duplicate bucket update error = %v", err)
	}
}
