package cliauth

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	conf "github.com/calypr/syfon/client/config"
	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

type stubManager struct {
	validFn func(*conf.Credential) (bool, error)
	saved   *conf.Credential
}

func (s *stubManager) Import(filePath, fenceToken string) (*conf.Credential, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubManager) Load(profile string) (*conf.Credential, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *stubManager) Save(cred *conf.Credential) error {
	copy := *cred
	s.saved = &copy
	return nil
}

func (s *stubManager) EnsureExists() error {
	return nil
}

func (s *stubManager) IsCredentialValid(cred *conf.Credential) (bool, error) {
	if s.validFn == nil {
		return true, nil
	}
	return s.validFn(cred)
}

func (s *stubManager) IsTokenValid(token string) (bool, error) {
	return true, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

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

func TestEnsureUsableProfileCredentialRefreshesExpiredAccessToken(t *testing.T) {
	t.Parallel()

	manager := &stubManager{
		validFn: func(cred *conf.Credential) (bool, error) {
			if cred.AccessToken == "new-token" {
				return true, nil
			}
			return false, fmt.Errorf("access_token is invalid but api_key is valid: token expired")
		},
	}
	cred := &conf.Credential{
		Profile:     "calypr",
		AccessToken: "old-token",
		APIKey:      "refresh-jwt",
		APIEndpoint: "https://example.test",
	}

	err := EnsureUsableProfileCredential(context.Background(), manager, cred, roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("expected POST refresh request, got %s", req.Method)
		}
		if got := req.URL.String(); got != "https://example.test/user/credentials/api/access_token" {
			t.Fatalf("unexpected refresh URL: %s", got)
		}
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Fatalf("read refresh body: %v", readErr)
		}
		if !strings.Contains(string(body), `"api_key":"refresh-jwt"`) {
			t.Fatalf("unexpected refresh body: %s", string(body))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(`{"access_token":"new-token"}`)),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	}))
	if err != nil {
		t.Fatalf("EnsureUsableProfileCredential returned error: %v", err)
	}
	if cred.AccessToken != "new-token" {
		t.Fatalf("expected refreshed token, got %q", cred.AccessToken)
	}
	if manager.saved == nil || manager.saved.AccessToken != "new-token" {
		t.Fatalf("expected refreshed credential to be saved, got %+v", manager.saved)
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
