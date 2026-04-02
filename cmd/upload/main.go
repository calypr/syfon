package upload

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var (
	uploadFile string
	uploadDid  string
)

var Cmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a local file via Syfon internal upload endpoints and register it",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if strings.TrimSpace(uploadFile) == "" {
			return fmt.Errorf("--file is required")
		}
		data, err := os.ReadFile(uploadFile)
		if err != nil {
			return err
		}
		requestedDID := strings.TrimSpace(uploadDid)
		did := requestedDID
		if did == "" {
			did = filepath.Base(uploadFile)
		}

		c := cliutil.NewSyfonClient(cmd)
		signed, err := c.RequestUploadURL(ctx, did)
		if err != nil {
			return err
		}
		serverGUID := strings.TrimSpace(signed.GUID)
		if requestedDID != "" && serverGUID != "" && serverGUID != requestedDID {
			return fmt.Errorf("server returned guid %q but --did %q was requested", serverGUID, requestedDID)
		}
		if serverGUID != "" {
			did = serverGUID
		}
		if strings.TrimSpace(signed.URL) == "" {
			return fmt.Errorf("server returned empty upload URL")
		}
		if err := cliutil.UploadBytesToSignedURL(ctx, signed.URL, data); err != nil {
			return err
		}

		sha := sha256.Sum256(data)
		sum := hex.EncodeToString(sha[:])
		objectURL, err := cliutil.CanonicalObjectURLFromSignedURL(signed.URL, strings.TrimSpace(signed.Bucket), did)
		if err != nil {
			return err
		}
		if err := cliutil.EnsureRecordWithURL(ctx, c, did, objectURL, filepath.Base(uploadFile), int64(len(data)), sum); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "uploaded %s as %s\n", uploadFile, did)
		fmt.Fprintf(cmd.OutOrStdout(), "sha256: %s\n", sum)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&uploadFile, "file", "", "Local file to upload")
	Cmd.Flags().StringVar(&uploadDid, "did", "", "Optional DID to require for the uploaded object (fails if server returns a different guid)")
}
