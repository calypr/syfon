package sha256sum

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var shaDID string

var Cmd = &cobra.Command{
	Use:   "sha256sum",
	Short: "Compute sha256 for an object and persist it in record metadata",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if strings.TrimSpace(shaDID) == "" {
			return fmt.Errorf("--did is required")
		}

		c := cliutil.NewSyfonClient(cmd)
		did := strings.TrimSpace(shaDID)
		signed, err := c.GetDownloadURL(ctx, did)
		if err != nil {
			return fmt.Errorf("get download url: %w", err)
		}
			downloadURL := strings.TrimSpace(signed.GetUrl())
			if downloadURL == "" {
				return fmt.Errorf("empty download url for did %s", did)
			}

			data, err := readURLBytes(ctx, downloadURL)
			if err != nil {
				return err
			}
		sumArr := sha256.Sum256(data)
		sum := hex.EncodeToString(sumArr[:])

		if err := cliutil.EnsureRecordWithURL(ctx, c, did, "", "", 0, sum); err != nil {
			return fmt.Errorf("persist sha256: %w", err)
		}

		fmt.Fprintln(cmd.OutOrStdout(), sum)
		return nil
	},
}

func readURLBytes(ctx context.Context, rawURL string) ([]byte, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("parse download url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "file":
		data, err := os.ReadFile(parsed.Path)
		if err != nil {
			return nil, fmt.Errorf("read file source: %w", err)
		}
		return data, nil
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, fmt.Errorf("build download request: %w", err)
		}
		resp, err := cliutil.NewHTTPClient().Do(req)
		if err != nil {
			return nil, fmt.Errorf("download request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return nil, fmt.Errorf("download failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("read download response: %w", err)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported download url scheme %q", parsed.Scheme)
	}
}

func init() {
	Cmd.Flags().StringVar(&shaDID, "did", "", "DRS object DID")
}
