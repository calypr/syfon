package cliauth

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

func TestServerClientOptionsBasicAuthValidation(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	username = "alice"
	if _, err := ServerClientOptions(); err == nil || !strings.Contains(err.Error(), "--username and --password must be set together") {
		t.Fatalf("expected missing password error, got %v", err)
	}
}

func TestServerClientOptionsConflictValidation(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	token = "tok"
	profile = "profile"
	if _, err := ServerClientOptions(); err == nil || !strings.Contains(err.Error(), "--token cannot be combined with --profile") {
		t.Fatalf("expected token/profile conflict, got %v", err)
	}
}

func TestServerClientOptionsUsesBasicAuth(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	username = "alice"
	password = "secret"
	opts, err := ServerClientOptions()
	if err != nil {
		t.Fatalf("ServerClientOptions returned error: %v", err)
	}

	cfg := syclient.DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.BasicAuth == nil || cfg.BasicAuth.Username != "alice" || cfg.BasicAuth.Password != "secret" {
		t.Fatalf("unexpected basic auth config: %+v", cfg.BasicAuth)
	}
}

func TestServerClientOptionsLoadsProfileToken(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	home := t.TempDir()
	t.Setenv("HOME", home)
	gen3Dir := filepath.Join(home, ".gen3")
	if err := os.MkdirAll(gen3Dir, 0o700); err != nil {
		t.Fatalf("mkdir .gen3: %v", err)
	}
	configPath := filepath.Join(gen3Dir, "gen3_client_config.ini")
	content := `[training]
access_token = test-token
api_endpoint = https://example.test
`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	profile = "training"
	opts, err := ServerClientOptions()
	if err != nil {
		t.Fatalf("ServerClientOptions returned error: %v", err)
	}

	cfg := syclient.DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	if got := strings.TrimSpace(cfg.Token); got != "test-token" {
		t.Fatalf("expected token from profile, got %q", got)
	}
}

func TestResolveServerURLUsesProfileEndpoint(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	t.Setenv("SYFON_SERVER_URL", "")
	t.Setenv("DRS_SERVER_URL", "")

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeProfileConfig(t, home, `[training]
access_token = test-token
api_endpoint = https://example.test/
`)

	profile = "training"
	cmd := testCommand()

	got, err := ResolveServerURL(cmd)
	if err != nil {
		t.Fatalf("ResolveServerURL returned error: %v", err)
	}
	if got != "https://example.test" {
		t.Fatalf("expected profile endpoint, got %q", got)
	}
}

func TestResolveServerURLPrefersExplicitServer(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	t.Setenv("SYFON_SERVER_URL", "")
	t.Setenv("DRS_SERVER_URL", "")

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeProfileConfig(t, home, `[training]
access_token = test-token
api_endpoint = https://example.test/
`)

	profile = "training"
	cmd := testCommand()
	if err := cmd.Root().PersistentFlags().Set("server", "https://override.test/"); err != nil {
		t.Fatalf("set server: %v", err)
	}

	got, err := ResolveServerURL(cmd)
	if err != nil {
		t.Fatalf("ResolveServerURL returned error: %v", err)
	}
	if got != "https://override.test" {
		t.Fatalf("expected explicit server override, got %q", got)
	}
}

func TestResolveServerURLPrefersEnvOverProfile(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeProfileConfig(t, home, `[training]
access_token = test-token
api_endpoint = https://example.test/
`)
	t.Setenv("SYFON_SERVER_URL", "https://env.test/")
	t.Setenv("DRS_SERVER_URL", "")

	profile = "training"
	cmd := testCommand()

	got, err := ResolveServerURL(cmd)
	if err != nil {
		t.Fatalf("ResolveServerURL returned error: %v", err)
	}
	if got != "https://env.test" {
		t.Fatalf("expected env server override, got %q", got)
	}
}

func TestResolveServerURLFallsBackToDefault(t *testing.T) {
	resetAuthState()
	t.Cleanup(resetAuthState)

	t.Setenv("SYFON_SERVER_URL", "")
	t.Setenv("DRS_SERVER_URL", "")

	cmd := testCommand()
	got, err := ResolveServerURL(cmd)
	if err != nil {
		t.Fatalf("ResolveServerURL returned error: %v", err)
	}
	if got != "http://127.0.0.1:8080" {
		t.Fatalf("expected localhost default, got %q", got)
	}
}

func resetAuthState() {
	profile = ""
	token = ""
	username = ""
	password = ""
}

func testCommand() *cobra.Command {
	root := &cobra.Command{Use: "syfon"}
	root.PersistentFlags().String("server", "http://127.0.0.1:8080", "")
	cmd := &cobra.Command{Use: "child"}
	root.AddCommand(cmd)
	return cmd
}

func writeProfileConfig(t *testing.T, home, content string) {
	t.Helper()

	gen3Dir := filepath.Join(home, ".gen3")
	if err := os.MkdirAll(gen3Dir, 0o700); err != nil {
		t.Fatalf("mkdir .gen3: %v", err)
	}
	configPath := filepath.Join(gen3Dir, "gen3_client_config.ini")
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}
