package repair

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/cmd/cliauth"
	sycommon "github.com/calypr/syfon/internal/common"
	repairsvc "github.com/calypr/syfon/internal/repair"
	"github.com/spf13/cobra"
)

type auditRunner func(cmd *cobra.Command, opts repairsvc.StorageCleanupAuditRequest) error
type applyRunner func(cmd *cobra.Command, opts repairsvc.StorageCleanupApplyRequest) error

type commandOptions struct {
	Organization          string
	Project               string
	PathPrefix            string
	ExpectedPathsFile     string
	RepoRoot              string
	CheckStorage          bool
	Format                string
	DeleteStaleDuplicates bool
	DeleteRepoOrphans     bool
	DryRun                bool
	SelectedPaths         []string
	SelectedObjectIDs     []string
	SelectedFindingKinds  []string
}

var Cmd = newCommand(runAudit, runApply)

func newCommand(audit auditRunner, apply applyRunner) *cobra.Command {
	var opts commandOptions
	cmd := &cobra.Command{
		Use:   "repair",
		Short: "Audit and apply Syfon storage cleanup actions",
	}
	addSharedFlags(cmd, &opts)
	cmd.AddCommand(&cobra.Command{
		Use:   "audit",
		Short: "Audit duplicate and orphaned storage paths",
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := normalizedAuditOptions(opts)
			if err != nil {
				return err
			}
			return audit(cmd, req)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "apply",
		Short: "Apply storage cleanup actions",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			req, err := normalizedApplyOptions(opts)
			if err != nil {
				return err
			}
			if req.Organization == "" || req.Project == "" {
				return fmt.Errorf("repair apply requires --organization and --project")
			}
			if !req.DeleteStaleDuplicates && !req.DeleteRepoOrphans {
				return fmt.Errorf("repair apply requires --delete-stale-duplicates and/or --delete-repo-orphans")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := normalizedApplyOptions(opts)
			if err != nil {
				return err
			}
			return apply(cmd, req)
		},
	})
	return cmd
}

func addSharedFlags(cmd *cobra.Command, opts *commandOptions) {
	cmd.PersistentFlags().StringVar(&opts.Organization, "organization", "", "Organization to scope the cleanup")
	cmd.PersistentFlags().StringVar(&opts.Project, "project", "", "Project to scope the cleanup")
	cmd.PersistentFlags().StringVar(&opts.PathPrefix, "path-prefix", "", "Optional path prefix under the project")
	cmd.PersistentFlags().StringVar(&opts.ExpectedPathsFile, "expected-paths-file", "", "File containing expected normalized paths (newline-delimited or JSON array)")
	cmd.PersistentFlags().StringVar(&opts.RepoRoot, "repo-root", "", "Local checkout root to walk for expected paths")
	cmd.PersistentFlags().BoolVar(&opts.CheckStorage, "check-storage", true, "Probe storage object existence for candidate records")
	cmd.PersistentFlags().StringVar(&opts.Format, "format", "text", "Output format: text or json")
	cmd.PersistentFlags().BoolVar(&opts.DeleteStaleDuplicates, "delete-stale-duplicates", false, "Delete stale duplicate Syfon records")
	cmd.PersistentFlags().BoolVar(&opts.DeleteRepoOrphans, "delete-repo-orphans", false, "Delete Syfon records and purge storage for repo-orphaned live objects")
	cmd.PersistentFlags().BoolVar(&opts.DryRun, "dry-run", false, "Report actions without deleting records")
	cmd.PersistentFlags().StringSliceVar(&opts.SelectedPaths, "selected-path", nil, "Limit apply to one or more normalized paths")
	cmd.PersistentFlags().StringSliceVar(&opts.SelectedObjectIDs, "selected-object-id", nil, "Limit apply to one or more Syfon object IDs")
	cmd.PersistentFlags().StringSliceVar(&opts.SelectedFindingKinds, "selected-finding-kind", nil, "Limit apply to one or more finding kinds")
}

func normalizedAuditOptions(opts commandOptions) (repairsvc.StorageCleanupAuditRequest, error) {
	expected, err := loadExpectedPaths(opts.ExpectedPathsFile, opts.RepoRoot)
	if err != nil {
		return repairsvc.StorageCleanupAuditRequest{}, err
	}
	checkStorage := opts.CheckStorage
	return repairsvc.StorageCleanupAuditRequest{
		Organization:  strings.TrimSpace(opts.Organization),
		Project:       strings.TrimSpace(opts.Project),
		PathPrefix:    strings.TrimSpace(opts.PathPrefix),
		ExpectedPaths: expected,
		CheckStorage:  &checkStorage,
	}, nil
}

func normalizedApplyOptions(opts commandOptions) (repairsvc.StorageCleanupApplyRequest, error) {
	expected, err := loadExpectedPaths(opts.ExpectedPathsFile, opts.RepoRoot)
	if err != nil {
		return repairsvc.StorageCleanupApplyRequest{}, err
	}
	checkStorage := opts.CheckStorage
	kinds := make([]repairsvc.FindingKind, 0, len(opts.SelectedFindingKinds))
	for _, kind := range opts.SelectedFindingKinds {
		kinds = append(kinds, repairsvc.FindingKind(strings.TrimSpace(kind)))
	}
	return repairsvc.StorageCleanupApplyRequest{
		Organization:          strings.TrimSpace(opts.Organization),
		Project:               strings.TrimSpace(opts.Project),
		PathPrefix:            strings.TrimSpace(opts.PathPrefix),
		ExpectedPaths:         expected,
		DeleteStaleDuplicates: opts.DeleteStaleDuplicates,
		DeleteRepoOrphans:     opts.DeleteRepoOrphans,
		DryRun:                opts.DryRun,
		SelectedPaths:         trimNonEmpty(opts.SelectedPaths),
		SelectedObjectIDs:     trimNonEmpty(opts.SelectedObjectIDs),
		SelectedFindingKinds:  kinds,
		CheckStorage:          &checkStorage,
	}, nil
}

func runAudit(cmd *cobra.Command, req repairsvc.StorageCleanupAuditRequest) error {
	client, err := buildClient(cmd)
	if err != nil {
		return err
	}
	report, err := client.Repair().StorageCleanupAudit(cmd.Context(), servicesAuditOptions(req))
	if err != nil {
		return err
	}
	return repairsvc.WriteStorageCleanupAudit(cmd.OutOrStdout(), normalizedFormat(cmd), report)
}

func runApply(cmd *cobra.Command, req repairsvc.StorageCleanupApplyRequest) error {
	client, err := buildClient(cmd)
	if err != nil {
		return err
	}
	result, err := client.Repair().StorageCleanupApply(cmd.Context(), servicesApplyOptions(req))
	if err != nil {
		return err
	}
	return repairsvc.WriteStorageCleanupApply(cmd.OutOrStdout(), normalizedFormat(cmd), result)
}

func buildClient(cmd *cobra.Command) (*syclient.Client, error) {
	client, err := cliauth.NewServerClient(cmd)
	if err != nil {
		return nil, err
	}
	concrete, ok := client.(*syclient.Client)
	if !ok {
		return nil, fmt.Errorf("unexpected syfon client type %T", client)
	}
	return concrete, nil
}

func normalizedFormat(cmd *cobra.Command) string {
	format, _ := cmd.Flags().GetString("format")
	format = strings.ToLower(strings.TrimSpace(format))
	if format == "" {
		return "text"
	}
	return format
}

func servicesAuditOptions(req repairsvc.StorageCleanupAuditRequest) services.StorageCleanupAuditOptions {
	return services.StorageCleanupAuditOptions{
		Organization:  req.Organization,
		ProjectID:     req.Project,
		PathPrefix:    req.PathPrefix,
		ExpectedPaths: append([]string(nil), req.ExpectedPaths...),
		CheckStorage:  req.CheckStorage,
	}
}

func servicesApplyOptions(req repairsvc.StorageCleanupApplyRequest) services.StorageCleanupApplyOptions {
	kinds := make([]string, 0, len(req.SelectedFindingKinds))
	for _, kind := range req.SelectedFindingKinds {
		kinds = append(kinds, string(kind))
	}
	return services.StorageCleanupApplyOptions{
		Organization:          req.Organization,
		ProjectID:             req.Project,
		PathPrefix:            req.PathPrefix,
		ExpectedPaths:         append([]string(nil), req.ExpectedPaths...),
		DeleteStaleDuplicates: req.DeleteStaleDuplicates,
		DeleteRepoOrphans:     req.DeleteRepoOrphans,
		DryRun:                req.DryRun,
		SelectedPaths:         append([]string(nil), req.SelectedPaths...),
		SelectedObjectIDs:     append([]string(nil), req.SelectedObjectIDs...),
		SelectedFindingKinds:  kinds,
		CheckStorage:          req.CheckStorage,
	}
}

func loadExpectedPaths(filePath, repoRoot string) ([]string, error) {
	filePath = strings.TrimSpace(filePath)
	repoRoot = strings.TrimSpace(repoRoot)
	if filePath != "" && repoRoot != "" {
		return nil, fmt.Errorf("--expected-paths-file and --repo-root are mutually exclusive")
	}
	switch {
	case filePath != "":
		return loadExpectedPathsFile(filePath)
	case repoRoot != "":
		return walkExpectedPaths(repoRoot)
	default:
		return nil, nil
	}
}

func loadExpectedPathsFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []string
	if json.Unmarshal(data, &raw) == nil {
		return normalizeManifestPaths(raw)
	}
	lines := strings.Split(string(data), "\n")
	return normalizeManifestPaths(lines)
}

func walkExpectedPaths(root string) ([]string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, 256)
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		normalized := filepath.ToSlash(rel)
		out = append(out, normalized)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return normalizeManifestPaths(out)
}

func normalizeManifestPaths(values []string) ([]string, error) {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		normalized, _, err := sycommon.NormalizeBrowsePath(raw)
		if err != nil {
			return nil, err
		}
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		out = append(out, normalized)
	}
	sort.Strings(out)
	return out, nil
}

func trimNonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
