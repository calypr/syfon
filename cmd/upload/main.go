package upload

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/xfer/upload"
	"github.com/google/uuid"
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

		did := strings.TrimSpace(uploadDID)
		if did == "" {
			did = uuid.NewString()
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

		// Create a DRS client from the SDK to pass to the orchestrator
		dc := drs.NewDrsClient(c.Requestor(), nil, c.Logger())

		// Set bucket/project if available (for future-proofing, currently uses defaults)
		// For now, we'll use the provided authz to infer scope if needed.

		drsObj := &drs.DRSObject{
			Id:   did,
			Name: filepath.Base(srcPath),
			Size: info.Size(),
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
		// This will automatically handle multipart (if > 4.5GB) and progress display.
		fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s (%s)...\n", srcPath, upload.FormatSize(info.Size()))

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
