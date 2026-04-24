package upload

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/xfer/upload"
	syfoncommon "github.com/calypr/syfon/common"
	"github.com/spf13/cobra"
)

var (
	uploadFile    string
	uploadDID     string
	uploadOrg     string
	uploadProject string
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

		org := strings.TrimSpace(uploadOrg)
		if org == "" {
			return fmt.Errorf("--org is required")
		}

		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}

		bucketName := ""
		if buckets, listErr := c.Buckets().List(ctx); listErr == nil {
			names := make([]string, 0, len(buckets.S3BUCKETS))
			for name := range buckets.S3BUCKETS {
				names = append(names, name)
			}
			sort.Strings(names)
			if len(names) > 0 {
				bucketName = names[0]
			}
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
		authzMap := syfoncommon.AuthzMapFromScope(org, strings.TrimSpace(uploadProject))

		am := drsapi.AccessMethod{Type: "s3"}
		if authzMap != nil {
			am.Authorizations = &authzMap
		}
		drsObj := &drsapi.DrsObject{
			Id:   did,
			Name: &name,
			Size: info.Size(),
			Checksums: []drsapi.Checksum{
				{Type: "sha256", Checksum: checksum},
			},
			AccessMethods: &[]drsapi.AccessMethod{am},
		}

		// Register and upload using the SDK's orchestrator
		fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s (%s)...\n", srcPath, upload.FormatSize(info.Size()))
		fmt.Fprintf(cmd.OutOrStdout(), "DID: %s\n", did)

		registered, err := upload.RegisterFile(ctx, c.Data(), c.DRS(), drsObj, srcPath, bucketName)
		if err != nil {
			return fmt.Errorf("upload failed: %w", err)
		}

		finalID := did
		if registered != nil && strings.TrimSpace(registered.Id) != "" {
			finalID = strings.TrimSpace(registered.Id)
		}
		if registered != nil && registered.AccessMethods != nil && len(*registered.AccessMethods) > 0 {
			objectURL := ""
			for _, am := range *registered.AccessMethods {
				if am.AccessUrl != nil && strings.TrimSpace(am.AccessUrl.Url) != "" {
					objectURL = strings.TrimSpace(am.AccessUrl.Url)
					break
				}
			}
			if objectURL != "" {
				authzList := syfoncommon.AuthzMapToList(authzMap)
				if err := c.Index().Upsert(ctx, finalID, objectURL, name, info.Size(), checksum, authzList); err != nil {
					return fmt.Errorf("sync index record: %w", err)
				}
			}
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\nsuccessfully uploaded %s\n", finalID)
		fmt.Fprintf(cmd.OutOrStdout(), "requested DID: %s\n", did)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&uploadFile, "file", "", "Path to source file")
	Cmd.Flags().StringVar(&uploadDID, "did", "", "Optional object DID (generated when omitted)")
	Cmd.Flags().StringVar(&uploadOrg, "org", "", "Required organization for the authz scope")
	Cmd.Flags().StringVar(&uploadProject, "project", "", "Optional project for the authz scope (omit for org-wide)")
}
