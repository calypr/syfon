package config

import (
	"fmt"
	"os"
	"strings"
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
	if cfg.Routes != (RoutesConfig{}) {
		t.Fatalf("expected route modules to default to disabled, got %+v", cfg.Routes)
	}
}

func TestLoadConfig_EnvOverrides(t *testing.T) {
	t.Setenv("DRS_PORT", "9090")
	t.Setenv("DRS_DB_SQLITE_FILE", "test_env.db")
	t.Setenv("DRS_AUTH_MODE", "local")
	t.Setenv("DRS_CREDENTIAL_LOCAL_KEY_FILE", "/tmp/test-env-kek")

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
	if cfg.CredentialEncryption.LocalKeyFile != "/tmp/test-env-kek" {
		t.Errorf("expected credential local key file override, got %s", cfg.CredentialEncryption.LocalKeyFile)
	}
}

func TestLoadConfig_CredentialEncryptionConfig(t *testing.T) {
	content := `
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
credential_encryption:
  local_key_file: ".syfon-credential-kek"
`
	tmpfile, err := os.CreateTemp("", "config-credential-encryption-*.yaml")
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

	cfg, err := LoadConfig(tmpfile.Name())
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if cfg.CredentialEncryption.LocalKeyFile != ".syfon-credential-kek" {
		t.Fatalf("expected configured local key file, got %q", cfg.CredentialEncryption.LocalKeyFile)
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

func TestLoadConfig_InvalidBucketNames(t *testing.T) {
	cases := []struct {
		bucket      string
		errContains string
	}{
		{"ab", "3-63 characters"},
		{strings.Repeat("a", 64), "3-63 characters"},
		{"MyBucket", "invalid"},
		{"my_bucket", "invalid"},
		{"my.bucket", "invalid"},
		{"-mybucket", "invalid"},
		{"mybucket-", "invalid"},
		{"192.168.1.1", "invalid"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.bucket, func(t *testing.T) {
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
`, tc.bucket)

			tmpfile, err := os.CreateTemp("", "config-invalid-bucket-*.yaml")
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

			_, err = LoadConfig(tmpfile.Name())
			if err == nil {
				t.Fatalf("expected error for invalid bucket %q, got nil", tc.bucket)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("bucket %q: expected error containing %q, got: %v", tc.bucket, tc.errContains, err)
			}
		})
	}
}

func TestLoadConfig_NonS3ProviderBucketNames(t *testing.T) {
	cases := []struct {
		provider string
		bucket   string
		want     string
	}{
		{"gcs", "my.gcs.bucket", "gcs"},
		{"gs", "my_bucket", "gcs"},
		{"azure", "my-azure-bucket", "azure"},
		{"azblob", "my-azure-bucket", "azure"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.provider+"/"+tc.bucket, func(t *testing.T) {
			content := fmt.Sprintf(`
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
s3_credentials:
  - bucket: %q
    provider: %q
`, tc.bucket, tc.provider)

			tmpfile, err := os.CreateTemp("", "config-non-s3-bucket-*.yaml")
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

			cfg, err := LoadConfig(tmpfile.Name())
			if err != nil {
				t.Fatalf("provider=%q bucket=%q: expected no error, got: %v", tc.provider, tc.bucket, err)
			}
			if len(cfg.S3Credentials) != 1 {
				t.Fatalf("provider=%q bucket=%q: expected one credential, got %d", tc.provider, tc.bucket, len(cfg.S3Credentials))
			}
			if cfg.S3Credentials[0].Provider != tc.want {
				t.Fatalf("provider=%q bucket=%q: expected normalized provider %q, got %q", tc.provider, tc.bucket, tc.want, cfg.S3Credentials[0].Provider)
			}
		})
	}
}

func TestLoadConfig_UnsupportedBucketProvider(t *testing.T) {
	content := `
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
s3_credentials:
  - bucket: "local-bucket"
    provider: "file"
`

	tmpfile, err := os.CreateTemp("", "config-unsupported-provider-*.yaml")
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

	_, err = LoadConfig(tmpfile.Name())
	if err == nil {
		t.Fatal("expected error for unsupported provider file")
	}
	if !strings.Contains(err.Error(), "unsupported provider") {
		t.Fatalf("expected unsupported provider error, got %v", err)
	}
}

func TestLoadConfig_BucketProviderValidationRegression(t *testing.T) {
	cases := []struct {
		name         string
		provider     string
		bucket       string
		wantProvider string
		wantErr      bool
		errSubstring string
	}{
		{
			name:         "gcs alias accepted",
			provider:     "gs",
			bucket:       "my.gcs.bucket",
			wantProvider: "gcs",
		},
		{
			name:         "azure alias accepted",
			provider:     "azblob",
			bucket:       "my-azure-bucket",
			wantProvider: "azure",
		},
		{
			name:         "file provider rejected",
			provider:     "file",
			bucket:       "local-bucket",
			wantErr:      true,
			errSubstring: "unsupported provider",
		},
		{
			name:         "gcs invalid bucket rejected",
			provider:     "gcs",
			bucket:       "192.168.1.1",
			wantErr:      true,
			errSubstring: "cannot be an IP address",
		},
		{
			name:         "azure invalid bucket rejected",
			provider:     "azure",
			bucket:       "my.azure.bucket",
			wantErr:      true,
			errSubstring: "invalid",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			content := fmt.Sprintf(`
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
s3_credentials:
  - bucket: %q
    provider: %q
`, tc.bucket, tc.provider)

			tmpfile, err := os.CreateTemp("", "config-bucket-regression-*.yaml")
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

			cfg, err := LoadConfig(tmpfile.Name())
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for provider=%q bucket=%q", tc.provider, tc.bucket)
				}
				if !strings.Contains(err.Error(), tc.errSubstring) {
					t.Fatalf("expected error containing %q, got %v", tc.errSubstring, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for provider=%q bucket=%q: %v", tc.provider, tc.bucket, err)
			}
			if len(cfg.S3Credentials) != 1 {
				t.Fatalf("expected one credential, got %d", len(cfg.S3Credentials))
			}
			if cfg.S3Credentials[0].Provider != tc.wantProvider {
				t.Fatalf("expected normalized provider %q, got %q", tc.wantProvider, cfg.S3Credentials[0].Provider)
			}
		})
	}
}

func TestLoadConfig_InvalidNonS3BucketNames(t *testing.T) {
	cases := []struct {
		provider    string
		bucket      string
		errContains string
	}{
		{"gcs", "192.168.1.1", "cannot be an IP address"},
		{"gcs", "goog-bucket", "cannot begin with \"goog\""},
		{"azure", "my.azure.bucket", "invalid"},
		{"azure", "my--bucket", "consecutive hyphens"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.provider+"/"+tc.bucket, func(t *testing.T) {
			content := fmt.Sprintf(`
auth:
  mode: local
database:
  sqlite:
    file: "test.db"
s3_credentials:
  - bucket: %q
    provider: %q
`, tc.bucket, tc.provider)

			tmpfile, err := os.CreateTemp("", "config-invalid-non-s3-bucket-*.yaml")
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

			_, err = LoadConfig(tmpfile.Name())
			if err == nil {
				t.Fatalf("expected error for provider=%q bucket=%q, got nil", tc.provider, tc.bucket)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Fatalf("provider=%q bucket=%q: expected error containing %q, got %v", tc.provider, tc.bucket, tc.errContains, err)
			}
		})
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

func TestLoadConfig_RouteEnvOverrides(t *testing.T) {
	t.Setenv("DRS_DB_SQLITE_FILE", "drs.db")
	t.Setenv("DRS_AUTH_MODE", "local")
	t.Setenv("DRS_ENABLE_GA4GH", "true")
	t.Setenv("DRS_ENABLE_INTERNAL", "1")
	t.Setenv("DRS_ENABLE_LFS", "true")
	t.Setenv("DRS_ENABLE_METRICS", "true")
	t.Setenv("DRS_ENABLE_DOCS", "true")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}
	if !cfg.Routes.Ga4gh || !cfg.Routes.Internal || !cfg.Routes.LFS || !cfg.Routes.Metrics || !cfg.Routes.Docs {
		t.Fatalf("expected all route flags to be enabled, got %+v", cfg.Routes)
	}
}
