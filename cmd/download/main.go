package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

var (
	downloadDID string
	downloadOut string
)

var Cmd = &cobra.Command{
	Use:   "download",
	Short: "Download an object to a local file",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		did := strings.TrimSpace(downloadDID)
		if did == "" {
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
		outPath := strings.TrimSpace(downloadOut)
		if outPath == "" {
			rec, err := c.Index().Get(ctx, did)
			if err != nil {
				return fmt.Errorf("resolve output filename from record: %w", err)
			}
			name := strings.TrimSpace(rec.GetFileName())
			if name == "" {
				name = did
			}
			outPath = name
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}

		signed, err := c.Data().DownloadURL(ctx, did, 0, false)
		if err != nil {
			return fmt.Errorf("get download url: %w", err)
		}
		downloadURL := strings.TrimSpace((&signed).GetUrl())
		if downloadURL == "" {
			return fmt.Errorf("empty download url for did %s", did)
		}
		if err := downloadURLToPath(ctx, downloadURL, outPath, c); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", did, outPath)
		return nil
	},
}

func downloadURLToPath(ctx context.Context, rawURL, outPath string, c *syclient.Client) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("parse download url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "file":
		data, err := os.ReadFile(parsed.Path)
		if err != nil {
			return fmt.Errorf("read file source: %w", err)
		}
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}
		return nil
	case "http", "https":
		rb := c.Requestor().New(http.MethodGet, rawURL)
		resp, err := c.Requestor().Do(ctx, rb)
		if err != nil {
			return fmt.Errorf("download request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			return fmt.Errorf("download failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read download response: %w", err)
		}
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}
		return nil
	default:
		return fmt.Errorf("unsupported download url scheme %q", parsed.Scheme)
	}
}

func init() {
	Cmd.Flags().StringVar(&downloadDID, "did", "", "DRS object DID")
	Cmd.Flags().StringVar(&downloadOut, "out", "", "Output file path (defaults to record file name)")
}
