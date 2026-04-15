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

	syclient "github.com/calypr/syfon/client"
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

		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		did := strings.TrimSpace(shaDID)
		signed, err := c.Data().DownloadURL(ctx, did, 0, false)
		if err != nil {
			return fmt.Errorf("get download url: %w", err)
		}
		downloadURL := ""
		if signed.Url != nil {
			downloadURL = strings.TrimSpace(*signed.Url)
		}
		if downloadURL == "" {
			return fmt.Errorf("empty download url for did %s", did)
		}

		data, err := readURLBytes(ctx, downloadURL, c)
		if err != nil {
			return err
		}
		sumArr := sha256.Sum256(data)
		sum := hex.EncodeToString(sumArr[:])

		// Fetch the latest record to preserve authorizations during upsert
		var authz []string
		if rec, err := c.Index().Get(ctx, did); err == nil {
			authz = rec.Authz
		}

		if err := c.Index().Upsert(ctx, did, "", "", 0, sum, authz); err != nil {
			return fmt.Errorf("persist sha256: %w", err)
		}

		fmt.Fprintln(cmd.OutOrStdout(), sum)
		return nil
	},
}

func readURLBytes(ctx context.Context, rawURL string, c *syclient.Client) ([]byte, error) {
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
		rb := c.Requestor().New(http.MethodGet, rawURL)
		resp, err := c.Requestor().Do(ctx, rb)
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
