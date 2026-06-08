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
	"github.com/calypr/syfon/client/request"
	syfonclient "github.com/calypr/syfon/client/services"
	transferdownload "github.com/calypr/syfon/client/transfer/download"
	syupload "github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/cmd/transferprogress"
	"github.com/calypr/syfon/internal/common"
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
		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}
		outPath := strings.TrimSpace(downloadOut)
		var expectedSize int64
		if outPath == "" {
			rec, err := c.Index().Get(ctx, did)
			if err != nil {
				return fmt.Errorf("resolve output filename from record: %w", err)
			}
			name := did
			if rec.FileName != nil {
				if pretty := common.DownloadFilename(*rec.FileName); pretty != "" {
					name = pretty
				}
			}
			if rec.Size != nil {
				expectedSize = *rec.Size
			}
			outPath = name
		} else {
			rec, err := c.Index().Get(ctx, did)
			if err == nil && rec.Size != nil {
				expectedSize = *rec.Size
			}
		}
		if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
			return fmt.Errorf("create output directory: %w", err)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Downloading %s -> %s", did, outPath)
		if expectedSize > 0 {
			fmt.Fprintf(cmd.OutOrStdout(), " (%s)", syupload.FormatSize(expectedSize))
		}
		fmt.Fprintln(cmd.OutOrStdout())

		progress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(outPath), expectedSize)
		progress.Start()
		downloadCtx := transferprogress.WithProgress(ctx, did, progress)

		if err := transferdownload.DownloadFile(downloadCtx, c.Data(), did, outPath); err != nil {
			progress.Abort()
			return err
		}
		progress.Finish()
		fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", did, outPath)
		return nil
	},
}

func downloadURLToPath(ctx context.Context, rawURL, outPath string, c syfonclient.SyfonClient) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("parse download url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "":
		srcPath := parsed.Path
		if srcPath == "" {
			srcPath = rawURL
		}
		data, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("read file source: %w", err)
		}
		if err := os.WriteFile(outPath, data, 0o644); err != nil {
			return fmt.Errorf("write output file: %w", err)
		}
		return nil
	case "http", "https":
		var resp *http.Response
		concrete, ok := c.(*syclient.Client)
		if !ok {
			return fmt.Errorf("client implementation does not support raw requests")
		}
		err := concrete.Requestor().Do(ctx, http.MethodGet, rawURL, nil, &resp, request.WithSkipAuth(true))
		if err != nil {
			return fmt.Errorf("download request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
			if err != nil {
				return fmt.Errorf("read error response body: %w", err)
			}
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
