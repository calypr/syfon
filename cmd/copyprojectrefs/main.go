package copyprojectrefs

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/syfon/apigen/client/internalapi"
	syclient "github.com/calypr/syfon/client"
	conf "github.com/calypr/syfon/client/config"
	"github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/spf13/cobra"
)

var (
	sourceProfile       string
	sourceToken         string
	sourceBasicUser     string
	sourceBasicPassword string
	targetServerURL     string
	targetProfile       string
	targetToken         string
	targetBasicUser     string
	targetBasicPassword string
	copyProjectRefsDid  string
)

var Cmd = &cobra.Command{
	Use:   "copy-project-refs <source-scope>",
	Short: "Copy project record references to another Syfon instance",
	Long: `Copy the exact Syfon records for one source scope to another Syfon instance.

The source scope is the org/project you are copying from, written as <organization>/<project-id>.

Use source-prefixed auth flags for the source Syfon instance:
  --source-profile
  --source-token
  --source-basic-user / --source-basic-password

Use target-prefixed auth flags for the destination Syfon instance:
  --target-profile
  --target-token
  --target-basic-user / --target-basic-password

For example, in:
  syfon copy-project-refs Ellrott_Lab/embedding_rotation --source-profile calypr-dev --target-server https://other-calypr.example --target-profile calypr-prod

the source scope is Ellrott_Lab/embedding_rotation.

This command copies metadata only: DID, access methods, controlled_access, hashes, filenames, size, description, version, and related record fields. It does not copy file bytes or modify bucket mappings.`,
	Example: `  syfon copy-project-refs Ellrott_Lab/embedding_rotation --source-profile calypr-dev --target-server https://other-calypr.example --target-profile calypr-prod
  syfon copy-project-refs Ellrott_Lab/embedding_rotation --target-server https://other-calypr.example -I fb4284fe-2c39-5939-bae9-69c4bf4c608a`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		scope, err := parseScopeArg(args[0], "source")
		if err != nil {
			return err
		}

		sourceClient, err := newSourceClient(ctx, cmd)
		if err != nil {
			return err
		}
		targetClient, err := newTargetClient(ctx)
		if err != nil {
			return err
		}

		records, err := recordsToCopy(ctx, cmd, sourceClient.Index(), scope, copyProjectRefsDid)
		if err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Found %d records to copy.\n", len(records))

		for i, rec := range records {
			fmt.Fprintf(cmd.OutOrStdout(), "[%d/%d] Copying record %s...\n", i+1, len(records), rec.Did)
			if err := upsertExactRecord(ctx, targetClient.Index(), rec); err != nil {
				return fmt.Errorf("failed to copy record %s: %w", rec.Did, err)
			}
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Successfully copied %d records for %s to %s.\n", len(records), args[0], strings.TrimRight(strings.TrimSpace(targetServerURL), "/"))
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&sourceProfile, "source-profile", "", "Gen3 profile for source reads; preferred over inherited --profile on this command")
	Cmd.Flags().StringVar(&sourceToken, "source-token", "", "Bearer token for source reads; overrides --source-profile")
	Cmd.Flags().StringVar(&sourceBasicUser, "source-basic-user", "", "Basic auth username for source Syfon reads")
	Cmd.Flags().StringVar(&sourceBasicPassword, "source-basic-password", "", "Basic auth password for source Syfon reads")
	Cmd.Flags().StringVar(&targetServerURL, "target-server", "", "Destination Syfon server base URL")
	Cmd.Flags().StringVar(&targetProfile, "target-profile", "", "Gen3 profile for target writes")
	Cmd.Flags().StringVar(&targetToken, "target-token", "", "Bearer token for target writes; overrides --target-profile")
	Cmd.Flags().StringVar(&targetBasicUser, "target-basic-user", "", "Basic auth username for target Syfon writes")
	Cmd.Flags().StringVar(&targetBasicPassword, "target-basic-password", "", "Basic auth password for target Syfon writes")
	Cmd.Flags().StringVarP(&copyProjectRefsDid, "individual", "I", "", "Copy only a single DID from the source scope")
}

func newSourceClient(ctx context.Context, cmd *cobra.Command) (services.SyfonClient, error) {
	serverURL, err := resolveSourceServerURL(ctx, cmd)
	if err != nil {
		return nil, err
	}
	opts, err := sourceClientOptions(ctx)
	if err != nil {
		return nil, err
	}
	return syclient.New(serverURL, opts...)
}

func newTargetClient(ctx context.Context) (services.SyfonClient, error) {
	serverURL, err := resolveTargetServerURL(ctx)
	if err != nil {
		return nil, err
	}
	opts, err := targetClientOptions(ctx)
	if err != nil {
		return nil, err
	}
	return syclient.New(serverURL, opts...)
}

func sourceClientOptions(ctx context.Context) ([]syclient.Option, error) {
	if !hasExplicitSourceAuthInputs() {
		return cliauth.ServerClientOptions()
	}
	return clientOptionsFromInputs(ctx, "source", sourceProfile, sourceToken, sourceBasicUser, sourceBasicPassword)
}

func targetClientOptions(ctx context.Context) ([]syclient.Option, error) {
	return clientOptionsFromInputs(ctx, "target", targetProfile, targetToken, targetBasicUser, targetBasicPassword)
}

func hasExplicitSourceAuthInputs() bool {
	return strings.TrimSpace(sourceProfile) != "" ||
		strings.TrimSpace(sourceToken) != "" ||
		strings.TrimSpace(sourceBasicUser) != "" ||
		strings.TrimSpace(sourceBasicPassword) != ""
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

func resolveTargetServerURL(ctx context.Context) (string, error) {
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
	return "", fmt.Errorf("--target-server is required unless --target-profile provides the destination api_endpoint")
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

func recordsToCopy(ctx context.Context, cmd *cobra.Command, index *services.IndexService, srcScope projectScope, individualDID string) ([]internalapi.InternalRecord, error) {
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
	return []internalapi.InternalRecord{internalRecordFromResponse(rec)}, nil
}

type projectScope struct {
	org     string
	project string
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

func recordMatchesScope(controlledAccess *[]string, srcScope projectScope) bool {
	resource := "/organization/" + srcScope.org + "/project/" + srcScope.project
	for _, candidate := range derefStringSlice(controlledAccess) {
		if strings.TrimSpace(candidate) == resource {
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

func internalRecordFromResponse(rec internalapi.InternalRecordResponse) internalapi.InternalRecord {
	return internalapi.InternalRecord{
		Did:              rec.Did,
		AccessMethods:    rec.AccessMethods,
		ControlledAccess: rec.ControlledAccess,
		CreatedTime:      rec.CreatedTime,
		Description:      rec.Description,
		FileName:         rec.FileName,
		Hashes:           rec.Hashes,
		Organization:     rec.Organization,
		Project:          rec.Project,
		Size:             rec.Size,
		UpdatedTime:      rec.UpdatedTime,
		Version:          rec.Version,
	}
}

func upsertExactRecord(ctx context.Context, index *services.IndexService, rec internalapi.InternalRecord) error {
	existing, err := index.Get(ctx, rec.Did)
	if err == nil {
		rec.CreatedTime = existing.CreatedTime
		rec.UpdatedTime = existing.UpdatedTime
		_, err = index.Update(ctx, rec.Did, rec)
		return err
	}
	return createRecord(ctx, index, rec)
}

func createRecord(ctx context.Context, index *services.IndexService, rec internalapi.InternalRecord) error {
	_, err := index.Create(ctx, rec)
	return err
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
