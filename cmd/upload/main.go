package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/cmd/transferprogress"

	clientaccess "github.com/calypr/syfon/client/access"
	intobjects "github.com/calypr/syfon/internal/objects"
	"github.com/spf13/cobra"
)

var (
	uploadFile      string
	uploadDID       string
	uploadOrg       string
	uploadProject   string
	uploadOverwrite bool
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
		checksum, err := hashFileSHA256(srcPath)
		if err != nil {
			return fmt.Errorf("read file for hashing: %w", err)
		}

		recordPath, err := uploadRecordPath(srcPath)
		if err != nil {
			return err
		}
		name := filepath.Base(srcPath)
		authzMap := clientaccess.AuthzMapFromScope(org, project)
		did := strings.TrimSpace(uploadDID)
		if did == "" {
			if project == "" {
				return fmt.Errorf("--project is required when --did is omitted")
			}
			minted, mintErr := intobjects.MintRecordIDFromChecksum(checksum, clientaccess.AuthzMapToControlledAccess(authzMap))
			if mintErr != nil {
				return mintErr
			}
			did = minted
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
			controlled := clientaccess.AuthzMapToControlledAccess(authzMap)
			drsObj.ControlledAccess = &controlled
		}
		overwrite, err := ensureWritableDID(ctx, c.DRS(), did, uploadOverwrite)
		if err != nil {
			return err
		}
		if overwrite.Warning != "" {
			fmt.Fprintf(cmd.ErrOrStderr(), "warning: %s\n", overwrite.Warning)
		}
		if drsObj.ControlledAccess == nil && overwrite.Existing != nil && overwrite.Existing.ControlledAccess != nil {
			controlled := append([]string(nil), (*overwrite.Existing.ControlledAccess)...)
			drsObj.ControlledAccess = &controlled
		}

		// Register and upload using the SDK's orchestrator
		fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s -> DID: %s\n", srcPath, did)

		progress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(srcPath), info.Size())
		progress.Start()
		uploadCtx := transferprogress.WithProgress(ctx, did, progress)

		var registered *drsapi.DrsObject
		if overwrite.ExpectedOldSHA != "" {
			registered, err = upload.ReplaceFile(uploadCtx, c.Data(), c.DRS(), drsObj, srcPath, bucketName, overwrite.ExpectedOldSHA)
		} else {
			registered, err = upload.RegisterFile(uploadCtx, c.Data(), c.DRS(), drsObj, srcPath, bucketName)
		}
		if err != nil {
			progress.Abort()
			return fmt.Errorf("upload failed: %w", err)
		}
		progress.Finish()

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
				if err := c.Index().Upsert(ctx, finalID, objectURL, recordPath, info.Size(), checksum, authzMap); err != nil {
					return fmt.Errorf("sync index record: %w", err)
				}
			}
		}
		fmt.Fprintf(cmd.OutOrStdout(), "\nsuccessfully uploaded %s\n", finalID)
		fmt.Fprintf(cmd.OutOrStdout(), "requested DID: %s\n", did)
		return nil
	},
}

func hashFileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func init() {
	Cmd.Flags().StringVar(&uploadFile, "file", "", "Path to source file")
	Cmd.Flags().StringVar(&uploadDID, "did", "", "Optional object DID (generated deterministically from sha256 + project scope when omitted)")
	Cmd.Flags().StringVar(&uploadOrg, "org", "", "Required organization for the authz scope")
	Cmd.Flags().StringVar(&uploadProject, "project", "", "Project for the authz scope (required when --did is omitted)")
	Cmd.Flags().BoolVar(&uploadOverwrite, "overwrite", false, "Allow replacing an existing DID's record and storage mapping")
}

type didLookup interface {
	GetObject(ctx context.Context, objectID string) (drsapi.DrsObject, error)
}

type overwriteInfo struct {
	Warning        string
	ExpectedOldSHA string
	Existing       *drsapi.DrsObject
}

func ensureWritableDID(ctx context.Context, drs didLookup, did string, overwrite bool) (overwriteInfo, error) {
	object, err := drs.GetObject(ctx, did)
	if err == nil {
		if !overwrite {
			return overwriteInfo{}, fmt.Errorf("object DID %s already exists; pass --overwrite to replace it", did)
		}
		sha, hasSHA := intobjects.CanonicalSHA256(object.Checksums)
		if !hasSHA {
			return overwriteInfo{}, fmt.Errorf("object DID %s has no valid SHA-256 checksum and cannot be safely replaced", did)
		}
		return overwriteInfo{
			Warning:        fmt.Sprintf("DID %s already exists; its metadata will be replaced only after the new upload succeeds.", did),
			ExpectedOldSHA: sha,
			Existing:       &object,
		}, nil
	}
	if errors.Is(err, errorapi.ErrNotFound) {
		return overwriteInfo{}, nil
	}
	return overwriteInfo{}, fmt.Errorf("check existing DID %s: %w", did, err)
}

func resolveUploadBucketForScope(buckets bucketapi.BucketsResponse, org, project string) (string, error) {
	org = strings.TrimSpace(org)
	project = strings.TrimSpace(project)
	scope, err := clientaccess.ResourcePath(org, project)
	if err != nil {
		return "", err
	}
	orgScope, err := clientaccess.ResourcePath(org, "")
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
	return clientaccess.NormalizeAccessResources(*meta.Programs)
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

func uploadRecordPath(srcPath string) (string, error) {
	cleaned := filepath.Clean(strings.TrimSpace(srcPath))
	if cleaned == "" || cleaned == "." {
		return "", fmt.Errorf("invalid upload file path %q", srcPath)
	}
	if !filepath.IsAbs(cleaned) {
		return filepath.ToSlash(cleaned), nil
	}

	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory for upload path: %w", err)
	}
	normalizedCWD := normalizeComparablePath(cwd)
	normalizedPath := normalizeComparablePath(cleaned)
	rel, err := filepath.Rel(normalizedCWD, normalizedPath)
	if err == nil {
		rel = filepath.Clean(rel)
		if rel != "." && rel != "" && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return filepath.ToSlash(rel), nil
		}
	}

	return filepath.Base(cleaned), nil
}

func normalizeComparablePath(p string) string {
	if abs, err := filepath.Abs(p); err == nil {
		p = abs
	}
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return resolved
	}
	return filepath.Clean(p)
}
