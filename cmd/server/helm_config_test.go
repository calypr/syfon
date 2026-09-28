package server

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/persistence/credentialcipher"
	"gopkg.in/yaml.v3"
)

func TestHelmCredentialConfiguration(t *testing.T) {
	chart := os.Getenv("SYFON_TEST_HELM_CHART")
	if chart == "" {
		t.Skip("set SYFON_TEST_HELM_CHART to verify a deployment chart")
	}
	for _, name := range []string{credentialcipher.CredentialMasterKeyEnv, credentialcipher.CredentialLocalKeyFileEnv, credentialcipher.DatabaseSQLiteFileEnv, credentialcipher.CredentialKeyManagerEnv, credentialcipher.CredentialKMSKeyIDEnv} {
		t.Setenv(name, "")
	}
	for name, value := range map[string]string{
		"DRS_DB_HOST": "postgres", "DRS_DB_PORT": "5432", "DRS_DB_USER": "syfon",
		"DRS_DB_PASSWORD": "test-password", "DRS_DB_DATABASE": "syfon",
		"DRS_DB_SSLMODE": "disable", "DRS_DB_ALLOW_INSECURE_TRANSPORT": "true",
		"DRS_DB_MAX_OPEN_CONNECTIONS": "25",
	} {
		t.Setenv(name, value)
	}
	const key = "0101010101010101010101010101010101010101010101010101010101010101"
	keyPath := filepath.Join(t.TempDir(), "kek")
	if err := os.WriteFile(keyPath, []byte(key), 0400); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, setting string }{
		{"nested master key", "config.credential_encryption.master_key=" + key},
		{"legacy chart shorthand", "credential_encryption.master_key=" + key},
		{"mounted key file", "config.credential_encryption.local_key_file=" + keyPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.Command("helm", "template", "syfon", chart, "--show-only", "templates/config-secret.yaml", "--set-string", tc.setting, "--set-string", "config.auth.fence_url=https://fence.example/user", "--set-string", "postgres.app.db_host=postgres", "--set-string", "postgres.app.db_port=5432")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("render Helm config: %v: %s", err, output)
			}
			var secret struct {
				StringData map[string]string `yaml:"stringData"`
			}
			if err := yaml.Unmarshal(output, &secret); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(configPath, []byte(secret.StringData["config.yaml"]), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.LoadConfig(configPath)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Profile != config.ProfileProduction || cfg.Database.Postgres == nil {
				t.Fatal("chart did not configure production PostgreSQL")
			}
			codec, err := credentialcipher.New(credentialEncryptionConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			encrypted, err := codec.EncryptField(context.Background(), "stored-credential")
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := credentialcipher.New(credentialEncryptionConfig(cfg))
			if err != nil {
				t.Fatal(err)
			}
			plain, err := restarted.DecryptField(context.Background(), encrypted)
			if err != nil || plain != "stored-credential" {
				t.Fatalf("credential after restart = %q, %v", plain, err)
			}
			if os.Getenv(credentialcipher.CredentialMasterKeyEnv) != "" || os.Getenv(credentialcipher.CredentialLocalKeyFileEnv) != "" {
				t.Fatal("config resolution mutated environment")
			}
		})
	}
}
