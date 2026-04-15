package upload

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/drs"
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
		srcPath, err := filepath.Abs(srcPath)
		if err != nil {
			return fmt.Errorf("resolve absolute path: %w", err)
		}
		info, err := os.Stat(srcPath)
		if err != nil {
			return fmt.Errorf("stat source file: %w", err)
		}
		if info.IsDir() {
			return fmt.Errorf("--file must be a regular file")
		}

		authz := strings.TrimSpace(uploadAuthz)
		//note: if authz is empty, will assume the server will allow
		//unauthenticated uploads. This is not recommended for production,
		//but can be useful for testing.

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

		// Create a DRS client from the SDK
		dc := drs.NewDrsClient(c.Requestor(), nil, c.Logger())

		drsObj := &drs.DRSObject{
			Id:   did,
			Name: filepath.Base(srcPath),
			Size: info.Size(),
			Checksums: []drs.Checksum{
				{
					Type:     "sha256",
					Checksum: checksum,
				},
			},
			AccessMethods: []drs.AccessMethod{
				{
					Type: "s3", // Default type
					Authorizations: drs.Authorizations{
						BearerAuthIssuers: []string{authz},
					},
				},
			},
		}

		// Register and upload using the SDK's orchestrator
		fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s (%s)...\n", srcPath, upload.FormatSize(info.Size()))
		fmt.Fprintf(cmd.OutOrStdout(), "DID: %s\n", did)

		_, err = upload.RegisterFile(ctx, c.Data(), dc, drsObj, srcPath, "")
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
