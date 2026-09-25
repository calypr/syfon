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

func TestCredentialIdentityMigrationRollsBackFailedCopy(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "failed-credential-copy.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE s3_credential (
		bucket TEXT PRIMARY KEY, provider TEXT NOT NULL DEFAULT 's3',
		region TEXT, access_key TEXT, secret_key TEXT, endpoint TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE bucket_scope (
		organization TEXT NOT NULL, project_id TEXT NOT NULL, bucket TEXT NOT NULL,
		path_prefix TEXT, PRIMARY KEY (organization, project_id)
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO s3_credential (bucket, access_key) VALUES (NULL, 'legacy-key')`); err != nil {
		t.Fatal(err)
	}
	if err := (&sqliteSchemaBootstrap{db: db}).ensureCredentialIdentitySchema(); err == nil {
		t.Fatal("migration accepted a legacy row with a null bucket")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var legacyRows, renamedTables int
	if err := db.QueryRow(`SELECT count(*) FROM s3_credential WHERE access_key = 'legacy-key'`).Scan(&legacyRows); err != nil {
		t.Fatalf("legacy table was not restored: %v", err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 's3_credential_legacy'`).Scan(&renamedTables); err != nil {
		t.Fatal(err)
	}
	if legacyRows != 1 || renamedTables != 0 {
		t.Fatalf("failed migration left rows=%d and renamed tables=%d", legacyRows, renamedTables)
	}
	if _, err := db.Exec(`UPDATE s3_credential SET bucket = 'recovered-bucket' WHERE access_key = 'legacy-key'`); err != nil {
		t.Fatal(err)
	}
	if err := (&sqliteSchemaBootstrap{db: db}).ensureCredentialIdentitySchema(); err != nil {
		t.Fatalf("retry migration: %v", err)
	}
	var credentialID string
	if err := db.QueryRow(`SELECT credential_id FROM s3_credential WHERE bucket = 'recovered-bucket'`).Scan(&credentialID); err != nil || credentialID != "recovered-bucket" {
		t.Fatalf("recovered credential ID = %q, error = %v", credentialID, err)
	}
}

func TestCredentialIdentityMigrationRecoversInterruptedLegacyCopy(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "interrupted-credential-copy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE s3_credential (
			credential_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, provider TEXT NOT NULL DEFAULT 's3',
			region TEXT, access_key TEXT, secret_key TEXT, endpoint TEXT
		)`,
		`CREATE TABLE s3_credential_legacy (
			bucket TEXT PRIMARY KEY, provider TEXT NOT NULL DEFAULT 's3',
			region TEXT, access_key TEXT, secret_key TEXT, endpoint TEXT
		)`,
		`INSERT INTO s3_credential_legacy (bucket, access_key, secret_key)
			VALUES ('legacy-bucket', 'legacy-key', 'legacy-secret')`,
		`CREATE TABLE bucket_scope (
			organization TEXT NOT NULL, project_id TEXT NOT NULL, bucket TEXT NOT NULL,
			path_prefix TEXT, PRIMARY KEY (organization, project_id)
		)`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap := &sqliteSchemaBootstrap{db: db}
	if err := bootstrap.ensureCredentialIdentitySchema(); err != nil {
		t.Fatalf("recover interrupted migration: %v", err)
	}
	if err := bootstrap.ensureCredentialIdentitySchema(); err != nil {
		t.Fatalf("repeat recovered migration: %v", err)
	}
	var accessKey string
	if err := db.QueryRow(`SELECT access_key FROM s3_credential WHERE credential_id = 'legacy-bucket'`).Scan(&accessKey); err != nil || accessKey != "legacy-key" {
		t.Fatalf("recovered access key = %q, error = %v", accessKey, err)
	}
	var renamedTables int
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 's3_credential_legacy'`).Scan(&renamedTables); err != nil || renamedTables != 0 {
		t.Fatalf("stranded legacy table count = %d, error = %v", renamedTables, err)
	}
}

func TestCredentialIdentityMigrationPreservesConflictingRecoveryData(t *testing.T) {
	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "conflicting-credential-copy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`CREATE TABLE s3_credential (credential_id TEXT PRIMARY KEY, bucket TEXT NOT NULL, provider TEXT NOT NULL DEFAULT 's3', region TEXT, access_key TEXT, secret_key TEXT, endpoint TEXT)`,
		`CREATE TABLE s3_credential_legacy (bucket TEXT PRIMARY KEY, provider TEXT NOT NULL DEFAULT 's3', region TEXT, access_key TEXT, secret_key TEXT, endpoint TEXT)`,
		`INSERT INTO s3_credential (credential_id, bucket, access_key) VALUES ('bucket', 'bucket', 'new-key')`,
		`INSERT INTO s3_credential_legacy (bucket, access_key) VALUES ('bucket', 'old-key')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&sqliteSchemaBootstrap{db: db}).ensureCredentialIdentitySchema(); err == nil || !strings.Contains(err.Error(), "conflicting legacy credentials") {
		t.Fatalf("conflicting recovery error = %v", err)
	}
	var legacyRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM s3_credential_legacy WHERE access_key = 'old-key'`).Scan(&legacyRows); err != nil || legacyRows != 1 {
		t.Fatalf("legacy rows after conflict = %d, error = %v", legacyRows, err)
	}
	if _, err := db.Exec(`UPDATE s3_credential SET credential_id = 'different', access_key = 'old-key' WHERE credential_id = 'bucket'`); err != nil {
		t.Fatal(err)
	}
	if err := (&sqliteSchemaBootstrap{db: db}).ensureCredentialIdentitySchema(); err == nil || !strings.Contains(err.Error(), "conflicting bucket assignments") {
		t.Fatalf("duplicate bucket recovery error = %v", err)
	}
}
