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

	"github.com/calypr/syfon/cmd/cliutil"
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
		c := cliutil.NewSyfonClient(cmd)
		outPath := strings.TrimSpace(downloadOut)
		if outPath == "" {
			rec, err := c.GetRecord(ctx, did)
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

		signed, err := c.GetDownloadURL(ctx, did)
		if err != nil {
			return fmt.Errorf("get download url: %w", err)
		}
			downloadURL := strings.TrimSpace(signed.GetUrl())
			if downloadURL == "" {
				return fmt.Errorf("empty download url for did %s", did)
			}
			if err := downloadURLToPath(ctx, downloadURL, outPath); err != nil {
				return err
			}
		fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", did, outPath)
		return nil
	},
}

func downloadURLToPath(ctx context.Context, rawURL, outPath string) error {
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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return fmt.Errorf("build download request: %w", err)
		}
		resp, err := cliutil.NewHTTPClient().Do(req)
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
