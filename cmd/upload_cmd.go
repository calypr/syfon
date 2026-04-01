package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
)

var (
	uploadFile string
	uploadDid  string
)

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a local file via Syfon internal upload endpoints and register it",
	RunE: func(cmd *cobra.Command, args []string) error {
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

		var signed struct {
			Guid   string `json:"guid"`
			Url    string `json:"url"`
			Bucket string `json:"bucket"`
		}
		if err := doJSON(http.MethodPost, "/data/upload", map[string]string{"guid": did}, &signed); err != nil {
			return err
		}
		serverGUID := strings.TrimSpace(signed.Guid)
		if requestedDID != "" && serverGUID != "" && serverGUID != requestedDID {
			return fmt.Errorf("server returned guid %q but --did %q was requested", serverGUID, requestedDID)
		}
		if serverGUID != "" {
			did = serverGUID
		}
		if strings.TrimSpace(signed.Url) == "" {
			return fmt.Errorf("server returned empty upload URL")
		}
		if err := uploadBytesToSignedURL(signed.Url, data); err != nil {
			return err
		}

		sha := sha256.Sum256(data)
		sum := hex.EncodeToString(sha[:])
		objectURL, err := canonicalObjectURLFromSignedURL(signed.Url, strings.TrimSpace(signed.Bucket), did)
		if err != nil {
			return err
		}
		if err := ensureRecordWithURL(did, objectURL, filepath.Base(uploadFile), int64(len(data)), sum); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "uploaded %s as %s\n", uploadFile, did)
		fmt.Fprintf(cmd.OutOrStdout(), "sha256: %s\n", sum)
		return nil
	},
}

func init() {
	uploadCmd.Flags().StringVar(&uploadFile, "file", "", "Local file to upload")
	uploadCmd.Flags().StringVar(&uploadDid, "did", "", "Optional DID to require for the uploaded object (fails if server returns a different guid)")
}
