package copyproject

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/apigen/client/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/client/drs"
	"github.com/calypr/syfon/apigen/client/internalapi"
	syclient "github.com/calypr/syfon/client"
	conf "github.com/calypr/syfon/client/config"
	"github.com/calypr/syfon/client/services"
	transferdownload "github.com/calypr/syfon/client/transfer/download"
	"github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/cmd/transferprogress"
	syfoncommon "github.com/calypr/syfon/common"
	internalcommon "github.com/calypr/syfon/internal/common"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "copy-project <source-scope> <destination-scope>",
	Short: "Copy project records and storage objects to a different scope",
	Long: `Copy a project's data from one scope to another.

The source scope is the org/project you are copying from, written as <organization>/<project-id>.
The destination scope is the org/project you are copying to, also written as <organization>/<project-id>.

Use source-prefixed auth flags for source reads when needed:
  --source-profile
  --source-token
  --source-basic-user / --source-basic-password

Use target-prefixed auth flags for destination writes when needed:
  --target-server
  --target-profile
  --target-token
  --target-basic-user / --target-basic-password

For example, in:
  syfon copy-project Ellrott_Lab/embedding_rotation CBDS_COLLAB/embedding_rotation

the source scope is Ellrott_Lab/embedding_rotation and the destination scope is CBDS_COLLAB/embedding_rotation.

The destination bucket is resolved on the destination Syfon instance when it already exists; otherwise Syfon creates the missing destination mappings and copies into a bucket with the same credential id as the source bucket, if that bucket is configured on the destination instance. Use -I/--individual to copy only one DID within the source scope.`,
	Example: `  syfon copy-project Ellrott_Lab/embedding_rotation CBDS_COLLAB/embedding_rotation
  syfon copy-project Ellrott_Lab/embedding_rotation CBDS_COLLAB/embedding_rotation --target-server https://other-calypr.example
  syfon copy-project Ellrott_Lab/embedding_rotation CBDS_COLLAB/embedding_rotation -I fb4284fe-2c39-5939-bae9-69c4bf4c608a`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		srcScope, err := parseScopeArg(args[0], "source")
		if err != nil {
			return err
		}
		dstScope, err := parseScopeArg(args[1], "destination")
		if err != nil {
			return err
		}

		sourceClient, sourceServer, err := newSourceClient(ctx, cmd)
		if err != nil {
			return err
		}
		targetClient, targetServer, err := newTargetClient(ctx, cmd)
		if err != nil {
			return err
		}

		sourceBuckets, err := sourceClient.Buckets().List(ctx)
		if err != nil {
			return fmt.Errorf("failed to list source buckets: %w", err)
		}
		targetBuckets, err := targetClient.Buckets().List(ctx)
		if err != nil {
			return fmt.Errorf("failed to list destination buckets: %w", err)
		}

		resolved, err := resolveCopyScopes(ctx, sourceClient.Buckets(), targetClient.Buckets(), sourceBuckets.S3BUCKETS, targetBuckets.S3BUCKETS, srcScope, dstScope)
		if err != nil {
			return err
		}

		if resolved.sourceProject == nil {
			return fmt.Errorf("source project scope %s/%s does not exist", srcScope.org, srcScope.project)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Source bucket resolved: %s\n", resolved.sourceBucket)

		if sameServerURL(sourceServer, targetServer) && srcScope == dstScope && resolved.sourceBucket == resolved.targetBucket {
			fmt.Fprintln(cmd.OutOrStdout(), "Source and destination scopes and buckets are identical. Nothing to copy.")
			return nil
		}

		if err := ensureDestinationScopes(ctx, cmd, targetClient.Buckets(), resolved); err != nil {
			return err
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Target bucket resolved: %s\n", resolved.targetBucket)
		records, err := recordsToCopy(ctx, cmd, sourceClient.Index(), srcScope)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Found %d records to copy.\n", len(records))

		tempDir := ".tmp"
		if err := os.MkdirAll(tempDir, 0o755); err != nil {
			return fmt.Errorf("failed to create temp directory: %w", err)
		}

		dstResource, err := syfoncommon.ResourcePath(dstScope.org, dstScope.project)
		if err != nil {
			return fmt.Errorf("failed to resolve target resource path: %w", err)
		}

		targetProjectPath := pathOrEmpty(resolved.targetProject)
		copiedCount := 0
		skippedCount := 0
		for i, rec := range records {
			if err := copyRecord(ctx, cmd, sourceClient, targetClient, rec, resolved.targetBucket, targetProjectPath, dstScope, dstResource, i+1, len(records), tempDir); err != nil {
				skippedCount++
				fmt.Fprintf(cmd.ErrOrStderr(), "warning: skipping %s: %v\n", rec.Did, err)
				continue
			}
			copiedCount++
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Successfully copied project %s/%s to %s/%s (%d copied, %d skipped, %d total).\n", srcScope.org, srcScope.project, dstScope.org, dstScope.project, copiedCount, skippedCount, len(records))
		return nil
	},
}

var (
	individualDID       string
	sourceProfile       string
	sourceToken         string
	sourceBasicUser     string
	sourceBasicPassword string
	targetServerURL     string
	targetProfile       string
	targetToken         string
	targetBasicUser     string
	targetBasicPassword string
)

type projectScope struct {
	org     string
	project string
}

type resolvedCopyScopes struct {
	source projectScope
	target projectScope

	sourceBucket string
	targetBucket string

	sourceProject *bucketapi.BucketScopeResponse
	sourceOrg     *bucketapi.BucketScopeResponse
	targetProject *bucketapi.BucketScopeResponse
	targetOrg     *bucketapi.BucketScopeResponse
}

func parseScopeArg(raw, label string) (projectScope, error) {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	if len(parts) != 2 {
		return projectScope{}, fmt.Errorf("invalid %s path %q: must be in format <organization>/<project-id>", label, raw)
	}
	scope := projectScope{
		org:     strings.TrimSpace(parts[0]),
		project: strings.TrimSpace(parts[1]),
	}
	if scope.org == "" || scope.project == "" {
		return projectScope{}, fmt.Errorf("%s scope must be non-empty and in <organization>/<project-id> format", label)
	}
	return scope, nil
}

func resolveCopyScopes(ctx context.Context, sourceBuckets, targetBuckets *services.BucketsService, sourceBucketMap, targetBucketMap map[string]bucketapi.BucketMetadata, srcScope, dstScope projectScope) (*resolvedCopyScopes, error) {
	resolved := &resolvedCopyScopes{
		source: srcScope,
		target: dstScope,
	}

	for bucketName := range sourceBucketMap {
		scopes, err := sourceBuckets.ListScopes(ctx, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to list source scopes for bucket %q: %w", bucketName, err)
		}
		for _, scope := range scopes {
			switch {
			case scope.Organization == srcScope.org && scope.ProjectId == srcScope.project:
				scopeCopy := scope
				resolved.sourceProject = &scopeCopy
				resolved.sourceBucket = bucketName
			case scope.Organization == srcScope.org && scope.ProjectId == "":
				scopeCopy := scope
				resolved.sourceOrg = &scopeCopy
			}
		}
	}

	for bucketName := range targetBucketMap {
		scopes, err := targetBuckets.ListScopes(ctx, bucketName)
		if err != nil {
			return nil, fmt.Errorf("failed to list destination scopes for bucket %q: %w", bucketName, err)
		}
		for _, scope := range scopes {
			switch {
			case scope.Organization == dstScope.org && scope.ProjectId == dstScope.project:
				scopeCopy := scope
				resolved.targetProject = &scopeCopy
				resolved.targetBucket = bucketName
			case scope.Organization == dstScope.org && scope.ProjectId == "":
				scopeCopy := scope
				resolved.targetOrg = &scopeCopy
				if resolved.targetBucket == "" {
					resolved.targetBucket = bucketName
				}
			}
		}
	}

	if resolved.sourceProject == nil {
		return resolved, nil
	}
	if resolved.sourceBucket == "" {
		return nil, fmt.Errorf("source project scope %s/%s exists but has no bucket mapping", srcScope.org, srcScope.project)
	}
	if resolved.targetBucket == "" {
		if _, ok := targetBucketMap[resolved.sourceBucket]; ok {
			resolved.targetBucket = resolved.sourceBucket
		} else {
			return nil, fmt.Errorf("destination scope %s/%s has no bucket mapping on the destination instance, and source bucket %q is not configured there", dstScope.org, dstScope.project, resolved.sourceBucket)
		}
	}
	return resolved, nil
}

func ensureDestinationScopes(ctx context.Context, cmd *cobra.Command, buckets *services.BucketsService, resolved *resolvedCopyScopes) error {
	if resolved.targetBucket == "" {
		return fmt.Errorf("failed to resolve a destination bucket for %s/%s", resolved.target.org, resolved.target.project)
	}

	if resolved.targetOrg == nil {
		orgPath := defaultOrgScopePath(resolved.targetBucket, resolved.target.org)
		if remapped, ok := remapOrgScopePath(resolved.sourceOrg, resolved.source.org, resolved.targetBucket, resolved.target.org); ok {
			orgPath = remapped
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Creating organization scope mapping on bucket %s: %s -> %s\n", resolved.targetBucket, resolved.target.org, orgPath)
		if err := buckets.AddScope(ctx, resolved.targetBucket, bucketapi.AddBucketScopeRequest{
			Organization: resolved.target.org,
			Path:         &orgPath,
		}); err != nil {
			return fmt.Errorf("failed to map organization scope on target bucket: %w", err)
		}

		resolved.targetOrg = &bucketapi.BucketScopeResponse{
			Organization: resolved.target.org,
			Path:         &orgPath,
		}
	}

	if resolved.targetProject != nil {
		return nil
	}

	projectPath := defaultProjectScopePath(resolved.targetBucket, resolved.target.org, resolved.target.project)
	if resolved.targetOrg != nil && resolved.targetOrg.Path != nil && strings.TrimSpace(*resolved.targetOrg.Path) != "" {
		projectPath = strings.TrimRight(strings.TrimSpace(*resolved.targetOrg.Path), "/") + "/" + resolved.target.project
	}
	if remapped, ok := remapProjectScopePath(resolved.sourceProject, resolved.sourceOrg, resolved.targetOrg, resolved.source, resolved.target, resolved.targetBucket); ok {
		projectPath = remapped
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Creating project scope mapping on bucket %s: %s/%s -> %s\n", resolved.targetBucket, resolved.target.org, resolved.target.project, projectPath)
	if err := buckets.AddScope(ctx, resolved.targetBucket, bucketapi.AddBucketScopeRequest{
		Organization: resolved.target.org,
		ProjectId:    resolved.target.project,
		Path:         &projectPath,
	}); err != nil {
		return fmt.Errorf("failed to map project scope on target bucket: %w", err)
	}

	resolved.targetProject = &bucketapi.BucketScopeResponse{
		Organization: resolved.target.org,
		ProjectId:    resolved.target.project,
		Path:         &projectPath,
	}
	return nil
}

func defaultOrgScopePath(bucket, org string) string {
	return fmt.Sprintf("s3://%s/organizations/%s", bucket, org)
}

func defaultProjectScopePath(bucket, org, project string) string {
	return fmt.Sprintf("s3://%s/organizations/%s/projects/%s", bucket, org, project)
}

func remapOrgScopePath(srcScope *bucketapi.BucketScopeResponse, srcOrg, targetBucket, targetOrg string) (string, bool) {
	if srcScope == nil || srcScope.Path == nil || strings.TrimSpace(*srcScope.Path) == "" {
		return "", false
	}
	u, segs, ok := parseStorageURL(*srcScope.Path)
	if !ok {
		return "", false
	}
	if len(segs) > 0 && segs[len(segs)-1] == srcOrg {
		segs[len(segs)-1] = targetOrg
	} else {
		segs = append(segs, targetOrg)
	}
	u.Host = targetBucket
	u.Path = "/" + path.Join(segs...)
	return u.String(), true
}

func remapProjectScopePath(srcProject, srcOrg, dstOrg *bucketapi.BucketScopeResponse, srcScope, dstScope projectScope, targetBucket string) (string, bool) {
	if dstOrg != nil && dstOrg.Path != nil && strings.TrimSpace(*dstOrg.Path) != "" && srcProject != nil && srcProject.Path != nil && strings.TrimSpace(*srcProject.Path) != "" {
		dstOrgURL, dstOrgSegs, okDst := parseStorageURL(*dstOrg.Path)
		srcProjURL, srcProjSegs, okProj := parseStorageURL(*srcProject.Path)
		srcOrgURL, srcOrgSegs, okOrg := parseStorageURL(pathOrEmpty(srcOrg))
		if okDst && okProj && okOrg && sameURLRoot(srcProjURL, srcOrgURL) && hasPathPrefix(srcProjSegs, srcOrgSegs) {
			relative := srcProjSegs[len(srcOrgSegs):]
			if len(relative) > 0 {
				dstOrgURL.Host = targetBucket
				dstOrgURL.Path = "/" + path.Join(append(dstOrgSegs, relative...)...)
				return dstOrgURL.String(), true
			}
		}
	}

	if srcProject == nil || srcProject.Path == nil || strings.TrimSpace(*srcProject.Path) == "" {
		return "", false
	}
	u, segs, ok := parseStorageURL(*srcProject.Path)
	if !ok {
		return "", false
	}
	switch {
	case len(segs) >= 2 && segs[len(segs)-2] == srcScope.org && segs[len(segs)-1] == srcScope.project:
		segs[len(segs)-2] = dstScope.org
		segs[len(segs)-1] = dstScope.project
	case len(segs) > 0 && segs[len(segs)-1] == srcScope.project:
		segs[len(segs)-1] = dstScope.project
	default:
		return "", false
	}
	u.Host = targetBucket
	u.Path = "/" + path.Join(segs...)
	return u.String(), true
}

func pathOrEmpty(scope *bucketapi.BucketScopeResponse) string {
	if scope == nil || scope.Path == nil {
		return ""
	}
	return strings.TrimSpace(*scope.Path)
}

func parseStorageURL(raw string) (*url.URL, []string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil, false
	}
	u, err := url.Parse(raw)
	if err != nil || strings.TrimSpace(u.Scheme) == "" || strings.TrimSpace(u.Host) == "" {
		return nil, nil, false
	}
	if internalcommon.ProviderFromScheme(u.Scheme) == "" {
		return nil, nil, false
	}
	trimmed := strings.Trim(strings.TrimSpace(u.Path), "/")
	if trimmed == "" {
		return u, nil, true
	}
	return u, strings.Split(trimmed, "/"), true
}

func sameURLRoot(a, b *url.URL) bool {
	if a == nil || b == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(a.Scheme), strings.TrimSpace(b.Scheme)) && strings.EqualFold(strings.TrimSpace(a.Host), strings.TrimSpace(b.Host))
}

func hasPathPrefix(candidate, prefix []string) bool {
	if len(prefix) > len(candidate) {
		return false
	}
	for i := range prefix {
		if candidate[i] != prefix[i] {
			return false
		}
	}
	return true
}

func copyRecord(ctx context.Context, cmd *cobra.Command, sourceClient, targetClient services.SyfonClient, rec internalapi.InternalRecord, targetBucket, targetProjectPath string, dstScope projectScope, dstResource string, current, total int, tempDir string) error {
	did := rec.Did
	fileName := ""
	if rec.FileName != nil {
		fileName = *rec.FileName
	}
	size := int64(0)
	if rec.Size != nil {
		size = *rec.Size
	}
	checksum := ""
	if rec.Hashes != nil {
		if sha256Val, ok := (*rec.Hashes)["sha256"]; ok {
			checksum = sha256Val
		}
	}

	fmt.Fprintf(cmd.OutOrStdout(), "[%d/%d] Copying %s (size: %d, name: %s)...\n", current, total, did, size, fileName)

	tempFile, err := os.CreateTemp(tempDir, "copy-project-*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %s: %w", tempPath, err)
	}
	defer os.Remove(tempPath)

	progressName := fileName
	if strings.TrimSpace(progressName) == "" {
		progressName = filepath.Base(tempPath)
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Downloading %s -> %s", did, tempPath)
	if size > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", upload.FormatSize(size))
	}
	fmt.Fprintln(cmd.OutOrStdout())

	downloadProgress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(progressName), size)
	downloadProgress.Start()
	downloadCtx := transferprogress.WithProgress(ctx, did, downloadProgress)
	if err := transferdownload.DownloadFile(downloadCtx, sourceClient.Data(), did, tempPath); err != nil {
		downloadProgress.Abort()
		return fmt.Errorf("failed to download file %s: %w", did, err)
	}
	downloadProgress.Finish()

	drsObj := &drsapi.DrsObject{
		Id:   did,
		Name: &fileName,
		Size: size,
		Checksums: []drsapi.Checksum{
			{Type: "sha256", Checksum: checksum},
		},
		ControlledAccess: &[]string{dstResource},
	}

	uploadKey := preferredUploadKey(rec.AccessMethods, checksum, fileName, tempPath)
	targetObjectURL := scopedObjectURL(targetProjectPath, targetBucket, uploadKey)

	fmt.Fprintf(cmd.OutOrStdout(), "Uploading %s -> %s", did, targetObjectURL)
	if size > 0 {
		fmt.Fprintf(cmd.OutOrStdout(), " (%s)", upload.FormatSize(size))
	}
	fmt.Fprintln(cmd.OutOrStdout())

	uploadProgress := transferprogress.New(cmd.OutOrStdout(), filepath.Base(progressName), size)
	uploadProgress.Start()
	uploadCtx := transferprogress.WithProgress(ctx, did, uploadProgress)
	if _, err := upload.RegisterFile(uploadCtx, targetClient.Data(), targetClient.DRS(), drsObj, tempPath, targetBucket); err != nil {
		uploadProgress.Abort()
		return fmt.Errorf("failed to upload file %s to target bucket %q: %w", did, targetBucket, err)
	}
	uploadProgress.Finish()

	targetAccessMethod := drsapi.AccessMethod{
		Type: drsapi.AccessMethodType(storageSchemeFromURL(targetObjectURL)),
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: targetObjectURL},
	}

	registerReq := drsapi.RegisterObjectsJSONRequestBody{
		Candidates: []drsapi.DrsObjectCandidate{{
			Name:             drsObj.Name,
			Size:             drsObj.Size,
			Checksums:        drsObj.Checksums,
			Aliases:          &[]string{"id:" + did},
			AccessMethods:    &[]drsapi.AccessMethod{targetAccessMethod},
			ControlledAccess: &[]string{dstResource},
			Description:      drsObj.Description,
			MimeType:         drsObj.MimeType,
			Version:          drsObj.Version,
		}},
	}
	if _, err := targetClient.DRS().RegisterObjects(ctx, registerReq); err != nil {
		return fmt.Errorf("failed to update DRS metadata for DID %s: %w", did, err)
	}

	authzOrg, authzProject := pathScope(dstResource)
	authzMap := syfoncommon.AuthzMapFromScope(authzOrg, authzProject)
	if err := targetClient.Index().Upsert(ctx, did, targetObjectURL, fileName, size, checksum, authzMap); err != nil {
		return fmt.Errorf("failed to sync index record for DID %s: %w", did, err)
	}

	return nil
}

func preferredUploadKey(accessMethods *[]drsapi.AccessMethod, checksum, fileName, filePath string) string {
	if strings.TrimSpace(checksum) != "" {
		return strings.TrimSpace(checksum)
	}
	key := path.Base(filePath)
	if accessMethods != nil {
		for _, am := range *accessMethods {
			if am.AccessUrl == nil || strings.TrimSpace(am.AccessUrl.Url) == "" {
				continue
			}
			parts := strings.Split(strings.TrimSpace(am.AccessUrl.Url), "/")
			if candidate := strings.TrimSpace(parts[len(parts)-1]); candidate != "" {
				key = candidate
				break
			}
		}
	}
	if strings.TrimSpace(fileName) != "" && key == path.Base(filePath) {
		key = strings.TrimSpace(fileName)
	}
	return key
}

func storageSchemeFromURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || strings.TrimSpace(parsed.Scheme) == "" {
		return "s3"
	}
	return strings.TrimSpace(parsed.Scheme)
}

func scopedObjectURL(projectPath, bucket, key string) string {
	projectPath = strings.TrimSpace(projectPath)
	key = strings.Trim(strings.TrimSpace(key), "/")
	if projectPath == "" {
		return fmt.Sprintf("s3://%s/%s", bucket, key)
	}
	parsed, err := url.Parse(projectPath)
	if err != nil || strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Sprintf("s3://%s/%s", bucket, key)
	}
	if key != "" {
		parsed.Path = "/" + path.Join(strings.Trim(strings.TrimSpace(parsed.Path), "/"), key)
	}
	return parsed.String()
}

func pathScope(resource string) (string, string) {
	org, project, ok := syfoncommon.ResourceScope(resource)
	if !ok {
		return "", ""
	}
	return org, project
}

func recordsToCopy(ctx context.Context, cmd *cobra.Command, index *services.IndexService, srcScope projectScope) ([]internalapi.InternalRecord, error) {
	if strings.TrimSpace(individualDID) == "" {
		fmt.Fprintf(cmd.OutOrStdout(), "Retrieving records for project %s/%s...\n", srcScope.org, srcScope.project)
		records, err := listAllProjectRecords(ctx, index, srcScope.org, srcScope.project)
		if err != nil {
			return nil, fmt.Errorf("failed to list project records: %w", err)
		}
		return records, nil
	}

	did := strings.TrimSpace(individualDID)
	fmt.Fprintf(cmd.OutOrStdout(), "Retrieving individual record %s from project %s/%s...\n", did, srcScope.org, srcScope.project)

	rec, err := index.Get(ctx, did)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve individual record %q: %w", did, err)
	}
	if !recordMatchesScope(rec.ControlledAccess, srcScope) {
		return nil, fmt.Errorf("record %s does not belong to source scope %s/%s", did, srcScope.org, srcScope.project)
	}

	return []internalapi.InternalRecord{{
		Did:              rec.Did,
		AccessMethods:    rec.AccessMethods,
		ControlledAccess: rec.ControlledAccess,
		Description:      rec.Description,
		FileName:         rec.FileName,
		Hashes:           rec.Hashes,
		Size:             rec.Size,
		Version:          rec.Version,
		Organization:     rec.Organization,
		Project:          rec.Project,
	}}, nil
}

func recordMatchesScope(controlledAccess *[]string, srcScope projectScope) bool {
	resource, err := syfoncommon.ResourcePath(srcScope.org, srcScope.project)
	if err != nil {
		return false
	}
	for _, candidate := range syfoncommon.NormalizeAccessResources(derefStringSlice(controlledAccess)) {
		if candidate == resource {
			return true
		}
	}
	return false
}

func derefStringSlice(values *[]string) []string {
	if values == nil {
		return nil
	}
	return *values
}

func listAllProjectRecords(ctx context.Context, index *services.IndexService, org, project string) ([]internalapi.InternalRecord, error) {
	var records []internalapi.InternalRecord
	queue := []string{""}
	seenDirs := map[string]struct{}{}
	seenDIDs := map[string]struct{}{}

	for len(queue) > 0 {
		currentPath := queue[0]
		queue = queue[1:]
		if _, ok := seenDirs[currentPath]; ok {
			continue
		}
		seenDirs[currentPath] = struct{}{}

		start := ""
		for {
			resp, err := index.List(ctx, services.ListRecordsOptions{
				Limit:        1000,
				Start:        start,
				Path:         currentPath,
				Organization: org,
				ProjectID:    project,
			})
			if err != nil {
				return nil, err
			}

			pageCount := 0
			if resp.Records != nil {
				for _, rec := range *resp.Records {
					did := strings.TrimSpace(rec.Did)
					if did == "" {
						continue
					}
					if _, ok := seenDIDs[did]; ok {
						continue
					}
					seenDIDs[did] = struct{}{}
					records = append(records, rec)
					pageCount++
					start = did
				}
			}

			if resp.Directories != nil {
				for _, dir := range *resp.Directories {
					nextPath := strings.TrimSpace(dir.Path)
					if nextPath == "" {
						continue
					}
					if _, ok := seenDirs[nextPath]; !ok {
						queue = append(queue, nextPath)
					}
				}
			}

			if pageCount == 0 || pageCount < 1000 {
				break
			}
		}
	}
	return records, nil
}

func init() {
	Cmd.Flags().StringVarP(&individualDID, "individual", "I", "", "Copy only a single DID from the source scope")
	Cmd.Flags().StringVar(&sourceProfile, "source-profile", "", "Gen3 profile for source reads; preferred over inherited --profile on this command")
	Cmd.Flags().StringVar(&sourceToken, "source-token", "", "Bearer token for source reads; overrides --source-profile")
	Cmd.Flags().StringVar(&sourceBasicUser, "source-basic-user", "", "Basic auth username for source Syfon reads")
	Cmd.Flags().StringVar(&sourceBasicPassword, "source-basic-password", "", "Basic auth password for source Syfon reads")
	Cmd.Flags().StringVar(&targetServerURL, "target-server", "", "Destination Syfon server base URL")
	Cmd.Flags().StringVar(&targetProfile, "target-profile", "", "Gen3 profile for destination writes")
	Cmd.Flags().StringVar(&targetToken, "target-token", "", "Bearer token for destination writes; overrides --target-profile")
	Cmd.Flags().StringVar(&targetBasicUser, "target-basic-user", "", "Basic auth username for destination Syfon writes")
	Cmd.Flags().StringVar(&targetBasicPassword, "target-basic-password", "", "Basic auth password for destination Syfon writes")
}

func newSourceClient(ctx context.Context, cmd *cobra.Command) (services.SyfonClient, string, error) {
	serverURL, err := resolveSourceServerURL(ctx, cmd)
	if err != nil {
		return nil, "", err
	}
	opts, err := sourceClientOptions(ctx)
	if err != nil {
		return nil, "", err
	}
	client, err := syclient.New(serverURL, opts...)
	if err != nil {
		return nil, "", err
	}
	return client, serverURL, nil
}

func newTargetClient(ctx context.Context, cmd *cobra.Command) (services.SyfonClient, string, error) {
	serverURL, err := resolveTargetServerURL(ctx, cmd)
	if err != nil {
		return nil, "", err
	}
	opts, err := targetClientOptions(ctx)
	if err != nil {
		return nil, "", err
	}
	client, err := syclient.New(serverURL, opts...)
	if err != nil {
		return nil, "", err
	}
	return client, serverURL, nil
}

func sourceClientOptions(ctx context.Context) ([]syclient.Option, error) {
	if !hasExplicitSourceAuthInputs() {
		return cliauth.ServerClientOptions()
	}
	return clientOptionsFromInputs(ctx, "source", sourceProfile, sourceToken, sourceBasicUser, sourceBasicPassword)
}

func targetClientOptions(ctx context.Context) ([]syclient.Option, error) {
	if !hasExplicitTargetAuthInputs() {
		return cliauth.ServerClientOptions()
	}
	return clientOptionsFromInputs(ctx, "target", targetProfile, targetToken, targetBasicUser, targetBasicPassword)
}

func hasExplicitSourceAuthInputs() bool {
	return strings.TrimSpace(sourceProfile) != "" ||
		strings.TrimSpace(sourceToken) != "" ||
		strings.TrimSpace(sourceBasicUser) != "" ||
		strings.TrimSpace(sourceBasicPassword) != ""
}

func hasExplicitTargetAuthInputs() bool {
	return strings.TrimSpace(targetProfile) != "" ||
		strings.TrimSpace(targetToken) != "" ||
		strings.TrimSpace(targetBasicUser) != "" ||
		strings.TrimSpace(targetBasicPassword) != ""
}

func resolveSourceServerURL(ctx context.Context, cmd *cobra.Command) (string, error) {
	if strings.TrimSpace(sourceProfile) != "" {
		credential, err := cliauth.LoadProfileCredential(ctx, sourceProfile)
		if err != nil {
			return "", err
		}
		serverURL := strings.TrimRight(strings.TrimSpace(credential.APIEndpoint), "/")
		if serverURL == "" {
			return "", fmt.Errorf("source profile %q has no api_endpoint", sourceProfile)
		}
		return serverURL, nil
	}
	return cliauth.ResolveServerURL(cmd)
}

func resolveTargetServerURL(ctx context.Context, cmd *cobra.Command) (string, error) {
	if serverURL := strings.TrimRight(strings.TrimSpace(targetServerURL), "/"); serverURL != "" {
		return serverURL, nil
	}
	if strings.TrimSpace(targetProfile) != "" {
		credential, err := cliauth.LoadProfileCredential(ctx, targetProfile)
		if err != nil {
			return "", err
		}
		serverURL := strings.TrimRight(strings.TrimSpace(credential.APIEndpoint), "/")
		if serverURL == "" {
			return "", fmt.Errorf("target profile %q has no api_endpoint", targetProfile)
		}
		return serverURL, nil
	}
	return cliauth.ResolveServerURL(cmd)
}

func clientOptionsFromInputs(ctx context.Context, side, profile, token, basicUser, basicPassword string) ([]syclient.Option, error) {
	basicUser = strings.TrimSpace(basicUser)
	basicPassword = strings.TrimSpace(basicPassword)
	token = strings.TrimSpace(token)
	profile = strings.TrimSpace(profile)
	if basicUser != "" || basicPassword != "" {
		if basicUser == "" || basicPassword == "" {
			return nil, fmt.Errorf("--%s-basic-user and --%s-basic-password must be set together", side, side)
		}
		if token != "" {
			return nil, fmt.Errorf("--%s-token cannot be combined with --%s-basic-user/--%s-basic-password", side, side, side)
		}
		if profile != "" {
			return nil, fmt.Errorf("--%s-profile cannot be combined with --%s-basic-user/--%s-basic-password", side, side, side)
		}
		return []syclient.Option{syclient.WithBasicAuth(basicUser, basicPassword)}, nil
	}
	if token != "" {
		if profile != "" {
			return nil, fmt.Errorf("--%s-token cannot be combined with --%s-profile", side, side)
		}
		return []syclient.Option{syclient.WithBearerToken(token)}, nil
	}
	if profile == "" {
		return nil, nil
	}
	credential, err := cliauth.LoadProfileCredential(ctx, profile)
	if err != nil {
		return nil, err
	}
	return optionsFromCredential(profile, credential)
}

func optionsFromCredential(profile string, credential *conf.Credential) ([]syclient.Option, error) {
	if credential == nil {
		return nil, fmt.Errorf("profile %q resolved to nil credential", profile)
	}
	if accessToken := strings.TrimSpace(credential.AccessToken); accessToken != "" {
		return []syclient.Option{syclient.WithBearerToken(accessToken)}, nil
	}
	keyID := strings.TrimSpace(credential.KeyID)
	apiKey := strings.TrimSpace(credential.APIKey)
	if keyID != "" && apiKey != "" {
		return []syclient.Option{syclient.WithBasicAuth(keyID, apiKey)}, nil
	}
	return nil, fmt.Errorf("profile %q has no access_token or key_id/api_key", profile)
}

func sameServerURL(left, right string) bool {
	return strings.EqualFold(strings.TrimRight(strings.TrimSpace(left), "/"), strings.TrimRight(strings.TrimSpace(right), "/"))
}
