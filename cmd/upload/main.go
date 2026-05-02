package upload

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/client/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/cliauth"
	syfoncommon "github.com/calypr/syfon/common"
	intcommon "github.com/calypr/syfon/internal/common"
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
		project := strings.TrimSpace(uploadProject)

		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}

		bucketName := ""
		if buckets, listErr := c.Buckets().List(ctx); listErr != nil {
			return fmt.Errorf("resolve bucket for scope: %w", listErr)
		} else {
			resolvedBucket, resolveErr := resolveUploadBucketForScope(buckets, org, project)
			if resolveErr != nil {
				return resolveErr
			}
			bucketName = resolvedBucket
		}

		// Calculate SHA256 hash so omitted DIDs can be minted deterministically from content+scope.
		fileBytes, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("read file for hashing: %w", err)
		}
		hash := sha256.Sum256(fileBytes)
		checksum := hex.EncodeToString(hash[:])

		name := filepath.Base(srcPath)
		authzMap := syfoncommon.AuthzMapFromScope(org, project)
		did := strings.TrimSpace(uploadDID)
		if did == "" {
			if project == "" {
				return fmt.Errorf("--project is required when --did is omitted")
			}
			did, err = intcommon.MintObjectIDFromChecksum(checksum, syfoncommon.AuthzMapToControlledAccess(authzMap))
			if err != nil {
				return err
			}
		}

		am := drsapi.AccessMethod{Type: "s3"}
		drsObj := &drsapi.DrsObject{
			Id:   did,
			Name: &name,
			Size: info.Size(),
			Checksums: []drsapi.Checksum{
				{Type: "sha256", Checksum: checksum},
			},
			AccessMethods: &[]drsapi.AccessMethod{am},
		}
		if authzMap != nil {
			controlled := syfoncommon.AuthzMapToControlledAccess(authzMap)
			drsObj.ControlledAccess = &controlled
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
				if err := c.Index().Upsert(ctx, finalID, objectURL, name, info.Size(), checksum, authzMap); err != nil {
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
	Cmd.Flags().StringVar(&uploadDID, "did", "", "Optional object DID (generated deterministically from sha256 + project scope when omitted)")
	Cmd.Flags().StringVar(&uploadOrg, "org", "", "Required organization for the authz scope")
	Cmd.Flags().StringVar(&uploadProject, "project", "", "Project for the authz scope (required when --did is omitted)")
}

func resolveUploadBucketForScope(buckets bucketapi.BucketsResponse, org, project string) (string, error) {
	org = strings.TrimSpace(org)
	project = strings.TrimSpace(project)
	scope, err := syfoncommon.ResourcePath(org, project)
	if err != nil {
		return "", err
	}
	orgScope, err := syfoncommon.ResourcePath(org, "")
	if err != nil {
		return "", err
	}

	exactMatches := make([]string, 0)
	orgWideMatches := make([]string, 0)
	for bucketName, meta := range buckets.S3BUCKETS {
		for _, resource := range normalizedBucketPrograms(meta) {
			switch resource {
			case scope:
				exactMatches = append(exactMatches, bucketName)
			case orgScope:
				orgWideMatches = append(orgWideMatches, bucketName)
			}
		}
	}

	sort.Strings(exactMatches)
	sort.Strings(orgWideMatches)
	exactMatches = uniqueStrings(exactMatches)
	orgWideMatches = uniqueStrings(orgWideMatches)

	if len(exactMatches) == 1 {
		return exactMatches[0], nil
	}
	if len(exactMatches) > 1 {
		return "", fmt.Errorf("scope %s maps to multiple buckets: %s", scope, strings.Join(exactMatches, ", "))
	}
	if project == "" {
		if len(orgWideMatches) == 1 {
			return orgWideMatches[0], nil
		}
		if len(orgWideMatches) > 1 {
			return "", fmt.Errorf("organization scope %s maps to multiple buckets: %s", orgScope, strings.Join(orgWideMatches, ", "))
		}
		return "", fmt.Errorf("no bucket configured for organization scope %s", orgScope)
	}
	if len(orgWideMatches) == 1 {
		return orgWideMatches[0], nil
	}
	if len(orgWideMatches) > 1 {
		return "", fmt.Errorf("project scope %s has no exact bucket mapping and organization scope %s maps to multiple buckets: %s", scope, orgScope, strings.Join(orgWideMatches, ", "))
	}
	return "", fmt.Errorf("no bucket configured for project scope %s or organization scope %s", scope, orgScope)
}

func normalizedBucketPrograms(meta bucketapi.BucketMetadata) []string {
	if meta.Programs == nil {
		return nil
	}
	return syfoncommon.NormalizeAccessResources(*meta.Programs)
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
