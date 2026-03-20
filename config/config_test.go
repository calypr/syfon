package config

import (
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
	os.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	os.Setenv("DRS_AUTH_MODE", "local")
	defer os.Unsetenv("DRS_DB_SQLITE_FILE")
	defer os.Unsetenv("DRS_AUTH_MODE")

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
	os.Setenv("DRS_PORT", "9090")
	os.Setenv("DRS_DB_SQLITE_FILE", "test_env.db")
	os.Setenv("DRS_AUTH_MODE", "local")
	defer func() {
		os.Unsetenv("DRS_PORT")
		os.Unsetenv("DRS_DB_SQLITE_FILE")
		os.Unsetenv("DRS_AUTH_MODE")
	}()

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
	os.Setenv("DRS_DB_HOST", "myhost")
	os.Setenv("DRS_DB_DATABASE", "mydb")
	os.Setenv("DRS_AUTH_MODE", "gen3")
	defer func() {
		os.Unsetenv("DRS_DB_HOST")
		os.Unsetenv("DRS_DB_DATABASE")
		os.Unsetenv("DRS_AUTH_MODE")
	}()

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
	os.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	defer os.Unsetenv("DRS_DB_SQLITE_FILE")

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error when auth.mode is not provided")
	}
}

func TestLoadConfig_InvalidAuthMode(t *testing.T) {
	os.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	os.Setenv("DRS_AUTH_MODE", "weird")
	defer func() {
		os.Unsetenv("DRS_DB_SQLITE_FILE")
		os.Unsetenv("DRS_AUTH_MODE")
	}()

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error for invalid auth mode")
	}
}

func TestLoadConfig_Gen3RequiresPostgres(t *testing.T) {
	os.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	os.Setenv("DRS_AUTH_MODE", "gen3")
	defer func() {
		os.Unsetenv("DRS_DB_SQLITE_FILE")
		os.Unsetenv("DRS_AUTH_MODE")
	}()

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected error when auth.mode=gen3 and postgres is not configured")
	}
}

func TestLoadConfig_InvalidDBPortEnv(t *testing.T) {
	os.Setenv("DRS_DB_HOST", "localhost")
	os.Setenv("DRS_DB_DATABASE", "drs")
	os.Setenv("DRS_DB_PORT", "not-a-number")
	os.Setenv("DRS_AUTH_MODE", "gen3")
	defer func() {
		os.Unsetenv("DRS_DB_HOST")
		os.Unsetenv("DRS_DB_DATABASE")
		os.Unsetenv("DRS_DB_PORT")
		os.Unsetenv("DRS_AUTH_MODE")
	}()

	if _, err := LoadConfig(""); err == nil {
		t.Fatal("expected invalid DRS_DB_PORT to return error")
	}
}

func TestLoadConfig_LFSEnvOverrides(t *testing.T) {
	os.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	os.Setenv("DRS_AUTH_MODE", "local")
	os.Setenv("DRS_LFS_MAX_BATCH_OBJECTS", "200")
	os.Setenv("DRS_LFS_MAX_BATCH_BODY_BYTES", "123456")
	os.Setenv("DRS_LFS_REQUEST_LIMIT_PER_MINUTE", "33")
	os.Setenv("DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE", "999")
	defer func() {
		os.Unsetenv("DRS_DB_SQLITE_FILE")
		os.Unsetenv("DRS_AUTH_MODE")
		os.Unsetenv("DRS_LFS_MAX_BATCH_OBJECTS")
		os.Unsetenv("DRS_LFS_MAX_BATCH_BODY_BYTES")
		os.Unsetenv("DRS_LFS_REQUEST_LIMIT_PER_MINUTE")
		os.Unsetenv("DRS_LFS_BANDWIDTH_LIMIT_BYTES_PER_MINUTE")
	}()

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
