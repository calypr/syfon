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
	defer os.Unsetenv("DRS_DB_SQLITE_FILE")

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
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	os.Setenv("DRS_PORT", "9090")
	os.Setenv("DRS_DB_SQLITE_FILE", "test_env.db")
	defer func() {
		os.Unsetenv("DRS_PORT")
		os.Unsetenv("DRS_DB_SQLITE_FILE")
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
	defer func() {
		os.Unsetenv("DRS_DB_HOST")
		os.Unsetenv("DRS_DB_DATABASE")
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
