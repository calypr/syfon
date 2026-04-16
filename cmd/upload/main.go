package upload

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/xfer/upload"
	"github.com/spf13/cobra"
)

var (
	uploadFile  string
	uploadDID   string
	uploadAuthz string
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

		authz := strings.TrimSpace(uploadAuthz)
		if authz == "" {
			return fmt.Errorf("--authz is required")
		}

		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}

		// Calculate SHA256 hash of the file to use as the content-addressable ID
		fileBytes, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("read file for hashing: %w", err)
		}
		hash := sha256.Sum256(fileBytes)
		checksum := hex.EncodeToString(hash[:])

		did := strings.TrimSpace(uploadDID)
		if did == "" {
			did = checksum
		}

		name := filepath.Base(srcPath)
		issuers := []string{authz}
		drsObj := &drsapi.DrsObject{
			Id:   did,
			Name: &name,
			Size: info.Size(),
			Checksums: []drsapi.Checksum{
				{
					Type:     "sha256",
					Checksum: checksum,
				},
			},
			AccessMethods: &[]drsapi.AccessMethod{
				{
					Type: "s3", // Default type
					Authorizations: &struct {
						BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
						DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
						PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
						SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
					}{
						BearerAuthIssuers: &issuers,
					},
				},
			},
		}

		// Register and upload using the SDK's orchestrator
		fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s (%s)...\n", srcPath, upload.FormatSize(info.Size()))
		fmt.Fprintf(cmd.OutOrStdout(), "DID: %s\n", did)

		_, err = upload.RegisterFile(ctx, c.Data(), c.DRS(), drsObj, srcPath, "")
		if err != nil {
			return fmt.Errorf("upload failed: %w", err)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "\nsuccessfully uploaded %s\n", did)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&uploadFile, "file", "", "Path to source file")
	Cmd.Flags().StringVar(&uploadDID, "did", "", "Optional object DID (generated when omitted)")
	Cmd.Flags().StringVar(&uploadAuthz, "authz", "", "Required authz scope for the record")
}
