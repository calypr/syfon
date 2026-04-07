package config

import (
	"fmt"
	"os"
	"testing"
)

func TestLoadConfig_NoDatabaseError(t *testing.T) {
	_, err := LoadConfig("")
	if err == nil {
		t.Error("expected error when no database is specified, got nil")
	}
}

func TestLoadConfig_MinimalValid(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	t.Setenv("DRS_AUTH_MODE", "local")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}

	if cfg.Database.Sqlite == nil {
		t.Fatal("expected sqlite config")
	}
	if cfg.LFS.MaxBatchObjects != DefaultLFSMaxBatchObjects {
		t.Fatalf("expected default lfs.max_batch_objects=%d, got %d", DefaultLFSMaxBatchObjects, cfg.LFS.MaxBatchObjects)
	}
	if cfg.LFS.MaxBatchBodyBytes != DefaultLFSMaxBatchBodyBytes {
		t.Fatalf("expected default lfs.max_batch_body_bytes=%d, got %d", DefaultLFSMaxBatchBodyBytes, cfg.LFS.MaxBatchBodyBytes)
	}
	if cfg.LFS.RequestLimitPerMinute != DefaultLFSRequestLimitPerMinute {
		t.Fatalf("expected default lfs.request_limit_per_minute=%d, got %d", DefaultLFSRequestLimitPerMinute, cfg.LFS.RequestLimitPerMinute)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("DRS_PORT", "9090")
	t.Setenv("DRS_DB_SQLITE_FILE", "test_env.db")
	t.Setenv("DRS_AUTH_MODE", "local")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Port)
	}

	if cfg.Database.Sqlite.File != "test_env.db" {
		t.Errorf("expected test_env.db, got %s", cfg.Database.Sqlite.File)
	}
}

func TestLoadConfig_PostgresEnv(t *testing.T) {
	t.Setenv("DRS_DB_HOST", "myhost")
	t.Setenv("DRS_DB_DATABASE", "mydb")
	t.Setenv("DRS_AUTH_MODE", "gen3")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Database.Postgres == nil {
		t.Fatal("expected postgres config to be initialized by env vars")
	}

	if cfg.Database.Postgres.Host != "myhost" {
		t.Errorf("expected host myhost, got %s", cfg.Database.Postgres.Host)
	}

	// Sqlite should be nil if postgres env vars are set (per my logic in config.go)
	// Wait, let's verify if my logic actually nils it out or if the validation fails.
}

func TestLoadConfig_MutualExclusivity(t *testing.T) {
	// Creating a temp yaml file with both
	content := `
database:
  sqlite:
    file: "foo.db"
  postgres:
    host: "localhost"
`
	tmpfile, err := os.CreateTemp("", "config*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.Write([]byte(content)); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	_, err = LoadConfig(tmpfile.Name())
	if err == nil {
		t.Error("expected error when both databases are specified, got nil")
	}
}

func TestLoadConfig_AuthModeRequired(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error when auth.mode is not provided")
	}
}

func TestLoadConfig_InvalidAuthMode(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	t.Setenv("DRS_AUTH_MODE", "weird")

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error for invalid auth mode")
	}
}

func TestLoadConfig_Gen3RequiresPostgres(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	t.Setenv("DRS_AUTH_MODE", "gen3")

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error when auth.mode=gen3 and postgres is not configured")
	}
}

func TestLoadConfig_InvalidDBPortEnv(t *testing.T) {
	t.Setenv("DRS_DB_HOST", "localhost")
	t.Setenv("DRS_DB_DATABASE", "drs")
	t.Setenv("DRS_DB_PORT", "not-a-number")
	t.Setenv("DRS_AUTH_MODE", "gen3")

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected invalid DRS_DB_PORT to return error")
	}
}

func TestLoadConfig_LFSEnvOverrides(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	t.Setenv("DRS_AUTH_MODE", "local")
	t.Setenv("DRS_LFS_MAX_BATCH_OBJECTS", "200")
	t.Setenv("DRS_LFS_MAX_BATCH_BODY_BYTES", "123456")
	t.Setenv("DRS_LFS_REQUEST_LIMIT_PER_MINUTE", "33")
	t.Setenv("DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE", "999")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.LFS.MaxBatchObjects != 200 {
		t.Fatalf("expected 200, got %d", cfg.LFS.MaxBatchObjects)
	}
	if cfg.LFS.MaxBatchBodyBytes != 123456 {
		t.Fatalf("expected 123456, got %d", cfg.LFS.MaxBatchBodyBytes)
	}
	if cfg.LFS.RequestLimitPerMinute != 33 {
		t.Fatalf("expected 33, got %d", cfg.LFS.RequestLimitPerMinute)
	}
	if cfg.LFS.BandwidthLimitBytesPerMinute != 999 {
		t.Fatalf("expected 999, got %d", cfg.LFS.BandwidthLimitBytesPerMinute)
	}
}

func TestLoadConfig_ValidBucketNames(t *testing.T) {
	validNames := []string{
		"abc",
		"my-bucket",
		"a1-b2-c3",
		"bucket123",
		"test-bucket-2026",
	}

	for _, bucket := range validNames {
		t.Run(bucket, func(t *testing.T) {
			content := fmt.Sprintf(`
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
s3_credentials:
  - bucket: %q
    provider: s3
    region: "us-east-1"
    access_key: "test-key"
    secret_key: "test-secret"
`, bucket)

			tmpfile, err := os.CreateTemp("", "config-valid-bucket-*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(content)); err != nil {
				t.Fatal(err)
			}
			if err := tmpfile.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := LoadConfig(tmpfile.Name()); err != nil {
				t.Fatalf("expected valid bucket %q to pass validation, got error: %v", bucket, err)
			}
		})
	}
}

