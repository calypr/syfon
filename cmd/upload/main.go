package upload

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
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var (
	uploadFile string
	uploadDID  string
)

var Cmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a file and register/update its DRS record",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if strings.TrimSpace(uploadFile) == "" {
			return fmt.Errorf("--file is required")
		}

		srcPath := strings.TrimSpace(uploadFile)
		info, err := os.Stat(srcPath)
		if err != nil {
			return fmt.Errorf("stat source file: %w", err)
		}
		if info.IsDir() {
			return fmt.Errorf("--file must be a regular file")
		}

		did := strings.TrimSpace(uploadDID)
		if did == "" {
			did = uuid.NewString()
		}

		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		uploadReq := syclient.UploadBlankRequest{}
		(&uploadReq).SetGuid(did)
		signed, err := c.Data().UploadBlank(ctx, uploadReq)
		if err != nil {
			return fmt.Errorf("request upload url: %w", err)
		}
		uploadURL := strings.TrimSpace((&signed).GetUrl())
		if uploadURL == "" {
			return fmt.Errorf("empty upload url for did %s", did)
		}

		if err := uploadBytesToURL(ctx, uploadURL, srcPath, c); err != nil {
			return err
		}

		objectURL, err := c.Data().CanonicalObjectURL(uploadURL, strings.TrimSpace((&signed).GetBucket()), did)
		if err != nil {
			return err
		}

		if err := c.Index().Upsert(ctx, did, objectURL, filepath.Base(srcPath), info.Size(), ""); err != nil {
			return fmt.Errorf("record update failed: %w", err)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "uploaded %s\n", did)
		return nil
	},
}

func uploadBytesToURL(ctx context.Context, rawURL, srcPath string, c *syclient.Client) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("parse upload url: %w", err)
	}
	switch strings.ToLower(parsed.Scheme) {
	case "file":
		content, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("read source file: %w", err)
		}
		dstPath := parsed.Path
		if dstPath == "" {
			return fmt.Errorf("invalid file upload url: %s", rawURL)
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return fmt.Errorf("create upload target dir: %w", err)
		}
		if err := os.WriteFile(dstPath, content, 0o644); err != nil {
			return fmt.Errorf("write uploaded file: %w", err)
		}
		return nil
	case "http", "https":
		f, err := os.Open(srcPath)
		if err != nil {
			return fmt.Errorf("open source file: %w", err)
		}
		defer f.Close()
		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat source file: %w", err)
		}

		rb := c.Requestor().New(http.MethodPut, rawURL)
		rb.WithBody(f)
		rb.WithPartSize(fi.Size())

		resp, err := c.Requestor().Do(ctx, rb)
		if err != nil {
			return fmt.Errorf("upload request failed: %w", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
			if err != nil {
				return fmt.Errorf("upload failed status=%d: %w", resp.StatusCode, err)
			}
			return fmt.Errorf("upload failed status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		return nil
	default:
		return fmt.Errorf("unsupported upload url scheme %q", parsed.Scheme)
	}
}

func init() {
	Cmd.Flags().StringVar(&uploadFile, "file", "", "Path to source file")
	Cmd.Flags().StringVar(&uploadDID, "did", "", "Optional object DID (generated when omitted)")
}
