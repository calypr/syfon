package config

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
)

// Test HIGH-1 fix: Basic auth warning in local mode
func TestLoadConfig_LocalModeWithoutBasicAuthWarning(t *testing.T) {
	oldStderr := os.Stderr
	defer func() { os.Stderr = oldStderr }()

	// Capture stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	// Clean up env vars
	defer func() {
		os.Unsetenv("DRS_AUTH_MODE")
		os.Unsetenv("DRS_BASIC_AUTH_USER")
		os.Unsetenv("DRS_BASIC_AUTH_PASSWORD")
		os.Unsetenv("DRS_DB_SQLITE_FILE")
	}()

	// Set local auth mode without basic auth
	os.Setenv("DRS_AUTH_MODE", "local")
	os.Unsetenv("DRS_BASIC_AUTH_USER")
	os.Unsetenv("DRS_BASIC_AUTH_PASSWORD")
	os.Setenv("DRS_DB_SQLITE_FILE", ":memory:")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if cfg.Auth.Mode != AuthModeLocal {
		t.Errorf("Auth.Mode = %q, want local", cfg.Auth.Mode)
	}

	if !bytes.Contains([]byte(output), []byte("WARNING")) {
		t.Errorf("Expected WARNING in stderr, got: %s", output)
	}
}

// Test HIGH-2 fix: Mock auth is supported in gen3 mode
func TestLoadConfig_MockAuthAllowsGen3Mode(t *testing.T) {
	defer func() {
		os.Unsetenv("DRS_AUTH_MODE")
		os.Unsetenv("DRS_AUTH_MOCK_ENABLED")
		os.Unsetenv("DRS_DB_HOST")
		os.Unsetenv("DRS_DB_DATABASE")
	}()

	os.Setenv("DRS_AUTH_MODE", "gen3")
	os.Setenv("DRS_AUTH_MOCK_ENABLED", "true")
	os.Setenv("DRS_DB_HOST", "localhost")
	os.Setenv("DRS_DB_DATABASE", "testdb")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig() unexpected error = %v", err)
	}
	if cfg.Auth.Mode != AuthModeGen3 {
		t.Errorf("Auth.Mode = %q, want gen3", cfg.Auth.Mode)
	}
}

// Test MED-2 fix: Postgres SSL mode defaults to "require"
func TestLoadConfig_PostgresSSLModeDefault(t *testing.T) {
	defer func() {
		os.Unsetenv("DRS_AUTH_MODE")
		os.Unsetenv("DRS_DB_HOST")
		os.Unsetenv("DRS_DB_DATABASE")
	}()

	os.Setenv("DRS_AUTH_MODE", "local")
	os.Setenv("DRS_DB_HOST", "localhost")
	os.Setenv("DRS_DB_DATABASE", "testdb")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if cfg.Database.Postgres == nil {
		t.Fatal("Database.Postgres is nil")
	}

	if cfg.Database.Postgres.SSLMode != "require" {
		t.Errorf("Postgres.SSLMode = %q, want require", cfg.Database.Postgres.SSLMode)
	}
}

// Test MED-1 fix: Secrets are redacted in JSON marshaling
func TestSecretRedaction_BasicAuthConfig(t *testing.T) {
	cfg := BasicAuthConfig{
		Username: "admin",
		Password: "super_secret_password",
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	// Check that password is not in the output
	output := string(data)
	if bytes.Contains([]byte(output), []byte("super_secret_password")) {
		t.Errorf("Password leaked in JSON output: %s", output)
	}
	if !bytes.Contains([]byte(output), []byte("***REDACTED***")) {
		t.Errorf("Password not redacted in JSON output: %s", output)
	}
}

func TestSecretRedaction_PostgresConfig(t *testing.T) {
	cfg := PostgresConfig{
		Host:     "db.example.com",
		Port:     5432,
		User:     "admin",
		Password: "db_secret_pass",
		Database: "mydb",
		SSLMode:  "require",
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	output := string(data)
	if bytes.Contains([]byte(output), []byte("db_secret_pass")) {
		t.Errorf("Password leaked in JSON output: %s", output)
	}
	if !bytes.Contains([]byte(output), []byte("***REDACTED***")) {
		t.Errorf("Password not redacted in JSON output: %s", output)
	}
}

func TestSecretRedaction_S3Config(t *testing.T) {
	cfg := S3Config{
		Bucket:    "my-bucket",
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
		SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		Endpoint:  "https://s3.amazonaws.com",
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	output := string(data)
	if bytes.Contains([]byte(output), []byte("wJalrXUtnFEMI")) {
		t.Errorf("SecretKey leaked in JSON output: %s", output)
	}
	if bytes.Contains([]byte(output), []byte("AKIAIOSFODNN7EXAMPLE")) {
		t.Errorf("AccessKey leaked in JSON output: %s", output)
	}

	// Redaction marker should appear twice (for both AccessKey and SecretKey)
	redactCount := bytes.Count([]byte(output), []byte("***REDACTED***"))
	if redactCount < 2 {
		t.Errorf("Expected at least 2 REDACTED markers, got %d: %s", redactCount, output)
	}
}
