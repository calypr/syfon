package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/persistence/credentialcipher"
)

type failingMigrationCipher struct{ calls int }

func (c *failingMigrationCipher) Enabled() (bool, error) { return true, nil }
func (c *failingMigrationCipher) Prepare(_ context.Context, credential *buckets.Credential) (*buckets.Credential, error) {
	c.calls++
	if c.calls == 2 {
		return nil, errors.New("injected encryption failure")
	}
	copy := *credential
	copy.AccessKey = "encrypted-" + copy.AccessKey
	copy.SecretKey = "encrypted-" + copy.SecretKey
	return &copy, nil
}
func (c *failingMigrationCipher) Parse(_ context.Context, credential *buckets.Credential) (*buckets.Credential, error) {
	return credential, nil
}

func TestOpenEncryptsExistingPlaintextCredentials(t *testing.T) {
	t.Setenv(credentialcipher.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credentials.db")
	db, err := NewSqliteDB(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.DB().ExecContext(ctx, `INSERT INTO s3_credential (credential_id, bucket, provider, region, access_key, secret_key, endpoint) VALUES (?, ?, ?, ?, ?, ?, ?)`, "legacy", "legacy", "s3", "us-east-1", "plain-ak", "plain-sk", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		db, err = NewSqliteDB(path, nil)
		if err != nil {
			t.Fatal(err)
		}
		var accessKey, secretKey string
		if err := db.DB().QueryRowContext(ctx, `SELECT access_key, secret_key FROM s3_credential WHERE credential_id = ?`, "legacy").Scan(&accessKey, &secretKey); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(accessKey, "enc:v2:") || !strings.HasPrefix(secretKey, "enc:v2:") {
			t.Fatalf("plaintext credentials remain after startup: access=%q secret=%q", accessKey, secretKey)
		}
		credential, err := db.GetS3Credential(ctx, "legacy")
		if err != nil {
			t.Fatal(err)
		}
		if credential.AccessKey != "plain-ak" || credential.SecretKey != "plain-sk" {
			t.Fatalf("decrypted credential = %+v", credential)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCredentialEncryptionMigrationRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "credentials.db")
	db, err := NewSqliteDB(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"first", "second"} {
		if _, err := db.DB().ExecContext(ctx, `INSERT INTO s3_credential (credential_id, bucket, provider, region, access_key, secret_key, endpoint) VALUES (?, ?, ?, ?, ?, ?, ?)`, id, id, "s3", "us-east-1", "plain-ak", "plain-sk", ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSqliteDB(path, &failingMigrationCipher{}); err == nil || !strings.Contains(err.Error(), "injected encryption failure") {
		t.Fatalf("failed migration error = %v", err)
	}
	raw, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var plaintextRows int
	if err := raw.QueryRowContext(ctx, `SELECT COUNT(*) FROM s3_credential WHERE access_key = 'plain-ak' AND secret_key = 'plain-sk'`).Scan(&plaintextRows); err != nil {
		t.Fatal(err)
	}
	if plaintextRows != 2 {
		t.Fatalf("failed migration left %d plaintext rows, want both rows unchanged", plaintextRows)
	}
}
