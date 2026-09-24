package postgres_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/persistence/credentialcipher"
	postgresdb "github.com/calypr/syfon/internal/persistence/postgres"
	"github.com/google/uuid"
)

func TestPostgresAutoBootstrapStopsAtSchemaDeadline(t *testing.T) {
	dsn := os.Getenv("SYFON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("SYFON_TEST_POSTGRES_DSN is not configured")
	}
	t.Setenv(credentialcipher.CredentialLocalKeyFileEnv, filepath.Join(t.TempDir(), "credential.key"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL test database: %v", err)
	}
	schema := "syfon_bootstrap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := admin.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		_ = admin.Close()
		t.Fatalf("create temporary schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		_ = admin.Close()
	})
	schemaDSN := postgresTestSchemaDSN(t, dsn, schema)

	initial, err := postgresdb.NewPostgresDB(ctx, schemaDSN, nil)
	if err != nil {
		t.Fatalf("initialize temporary schema: %v", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatalf("close initialized schema store: %v", err)
	}

	lockDB, err := sql.Open("postgres", schemaDSN)
	if err != nil {
		t.Fatalf("open schema lock connection: %v", err)
	}
	t.Cleanup(func() { _ = lockDB.Close() })
	tx, err := lockDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin schema lock transaction: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `LOCK TABLE s3_credential IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("lock s3_credential: %v", err)
	}

	result := make(chan error, 1)
	started := time.Now()
	go func() {
		database, err := postgresdb.NewPostgresDBWithOptions(context.Background(), schemaDSN, nil, postgresdb.OpenOptions{
			SchemaMode:             postgresdb.SchemaModeAuto,
			PingTimeout:            time.Second,
			SchemaBootstrapTimeout: 200 * time.Millisecond,
		})
		if database != nil {
			_ = database.Close()
		}
		result <- err
	}()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("automatic schema bootstrap succeeded while a required table was locked")
		}
		if elapsed := time.Since(started); elapsed > 2*time.Second {
			t.Fatalf("schema bootstrap returned after %s, want deadline within 2s: %v", elapsed, err)
		}
	case <-time.After(2 * time.Second):
		_ = tx.Rollback()
		select {
		case err := <-result:
			t.Fatalf("schema bootstrap ignored its deadline and returned only after releasing the lock: %v", err)
		case <-time.After(2 * time.Second):
			t.Fatal("schema bootstrap remained blocked after releasing the lock")
		}
	}
}
