package cliauth

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	syclient "github.com/calypr/syfon/client"
	conf "github.com/calypr/syfon/client/config"
	syfonclient "github.com/calypr/syfon/client/services"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	profile  string
	token    string
	username string
	password string
)

func RegisterRootFlags(fs *pflag.FlagSet) {
	fs.StringVar(&profile, "profile", strings.TrimSpace(os.Getenv("SYFON_PROFILE")), "Gen3 profile from ~/.gen3/gen3_client_config.ini")
	fs.StringVar(&token, "token", strings.TrimSpace(os.Getenv("SYFON_TOKEN")), "Bearer token for authenticated Syfon servers")
	fs.StringVar(&username, "username", strings.TrimSpace(os.Getenv("SYFON_USERNAME")), "Basic auth username for authenticated Syfon servers")
	fs.StringVar(&password, "password", strings.TrimSpace(os.Getenv("SYFON_PASSWORD")), "Basic auth password for authenticated Syfon servers")
}

func NewServerClient(cmd *cobra.Command) (syfonclient.SyfonClient, error) {
	serverURL, err := ResolveServerURL(cmd)
	if err != nil {
		return nil, err
	}
	opts, err := ServerClientOptions()
	if err != nil {
		return nil, err
	}
	return syclient.New(serverURL, opts...)
}

func ResolveServerURL(cmd *cobra.Command) (string, error) {
	flag := cmd.Root().PersistentFlags().Lookup("server")
	if flag == nil {
		return "", fmt.Errorf("--server flag not found")
	}
	if flag.Changed {
		serverURL := strings.TrimRight(strings.TrimSpace(flag.Value.String()), "/")
		if serverURL == "" {
			return "", fmt.Errorf("--server cannot be empty")
		}
		return serverURL, nil
	}

	if envServer := strings.TrimSpace(os.Getenv("SYFON_SERVER_URL")); envServer != "" {
		return strings.TrimRight(envServer, "/"), nil
	}
	if envServer := strings.TrimSpace(os.Getenv("DRS_SERVER_URL")); envServer != "" {
		return strings.TrimRight(envServer, "/"), nil
	}

	resolvedProfile := strings.TrimSpace(profile)
	if resolvedProfile != "" {
		credential, err := loadProfileCredential(resolvedProfile)
		if err != nil {
			return "", err
		}
		serverURL := strings.TrimRight(strings.TrimSpace(credential.APIEndpoint), "/")
		if serverURL == "" {
			return "", fmt.Errorf("profile %q has no api_endpoint", resolvedProfile)
		}
		return serverURL, nil
	}

	serverURL := strings.TrimRight(strings.TrimSpace(flag.Value.String()), "/")
	if serverURL == "" {
		return "", fmt.Errorf("--server cannot be empty")
	}
	return serverURL, nil
}

func ServerClientOptions() ([]syclient.Option, error) {
	resolvedProfile := strings.TrimSpace(profile)
	resolvedToken := strings.TrimSpace(token)
	resolvedUsername := strings.TrimSpace(username)
	resolvedPassword := strings.TrimSpace(password)

	if resolvedUsername != "" || resolvedPassword != "" {
		if resolvedUsername == "" || resolvedPassword == "" {
			return nil, fmt.Errorf("--username and --password must be set together")
		}
		if resolvedToken != "" {
			return nil, fmt.Errorf("--token cannot be combined with --username/--password")
		}
		if resolvedProfile != "" {
			return nil, fmt.Errorf("--profile cannot be combined with --username/--password")
		}
		return []syclient.Option{syclient.WithBasicAuth(resolvedUsername, resolvedPassword)}, nil
	}

	if resolvedToken != "" {
		if resolvedProfile != "" {
			return nil, fmt.Errorf("--token cannot be combined with --profile")
		}
		return []syclient.Option{syclient.WithBearerToken(resolvedToken)}, nil
	}

	if resolvedProfile == "" {
		return nil, nil
	}

	credential, err := loadProfileCredential(resolvedProfile)
	if err != nil {
		return nil, err
	}
	if accessToken := strings.TrimSpace(credential.AccessToken); accessToken != "" {
		return []syclient.Option{syclient.WithBearerToken(accessToken)}, nil
	}
	keyID := strings.TrimSpace(credential.KeyID)
	apiKey := strings.TrimSpace(credential.APIKey)
	if keyID != "" && apiKey != "" {
		return []syclient.Option{syclient.WithBasicAuth(keyID, apiKey)}, nil
	}
	return nil, fmt.Errorf("profile %q has no access_token or complete key_id/api_key pair", resolvedProfile)
}

func loadProfileCredential(profile string) (*conf.Credential, error) {
	manager := conf.NewConfigure(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	return manager.Load(profile)
}
