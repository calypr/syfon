package migratecmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/migrate"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "migrate",
	Short: "Export and import Gen3 Indexd records for Syfon migration",
	Long:  "Export records from a Gen3 Indexd-compatible /index API into a local SQLite dump, then import that dump into Syfon using the existing /index/bulk loader.",
}

var (
	indexdURL     string
	dumpPath      string
	sourceProfile string
	targetProfile string
	sourceToken   string
	targetToken   string
	batchSize     int
	limit         int
	sweeps        int
	dryRun        bool
	defaultAuthz  []string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export records from Gen3 Indexd into a SQLite dump",
	RunE: func(cmd *cobra.Command, args []string) error {
		sourceURL := strings.TrimRight(strings.TrimSpace(indexdURL), "/")
		if sourceURL == "" {
			return fmt.Errorf("--indexd-url is required")
		}
		if strings.TrimSpace(dumpPath) == "" {
			return fmt.Errorf("--dump is required")
		}

		sourceAuth, err := authFromInputs(sourceProfile, sourceToken)
		if err != nil {
			return fmt.Errorf("source auth: %w", err)
		}
		httpClient := &http.Client{Timeout: 90 * time.Second}
		source, err := migrate.NewHTTPClient(sourceURL, sourceAuth, httpClient)
		if err != nil {
			return err
		}
		dump, err := migrate.OpenExistingSQLiteDump(dumpPath)
		if err != nil {
			return err
		}
		defer dump.Close()

		if dryRun {
			cmd.Println("dry-run: no records will be written to the dump")
		}
		cmd.Printf("migration export starting: indexd=%s dump=%s batch=%d limit=%d sweeps=%d\n", sourceURL, dumpPath, batchSize, limit, sweeps)
		stats, err := migrate.Run(context.Background(), source, dump, migrate.Config{
			BatchSize:    batchSize,
			Limit:        limit,
			DryRun:       dryRun,
			DefaultAuthz: defaultAuthz,
			Sweeps:       sweeps,
		})
		if err != nil {
			return err
		}
		count, countErr := dump.Count(context.Background())
		if countErr != nil {
			return countErr
		}
		cmd.Printf("migration export complete: fetched=%d transformed=%d dumped=%d skipped=%d errors=%d unique_ids=%d dump_records=%d\n",
			stats.Fetched,
			stats.Transformed,
			stats.Loaded,
			stats.Skipped,
			stats.Errors,
			stats.CountOfUniqueIDs,
			count,
		)
		return nil
	},
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import a migration SQLite dump into Syfon",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(dumpPath) == "" {
			return fmt.Errorf("--dump is required")
		}
		targetURL, err := targetServerURL(cmd)
		if err != nil {
			return err
		}
		if strings.TrimSpace(targetProfile) == "" {
			targetProfile = sourceProfile
		}
		targetAuth, err := authFromInputs(targetProfile, targetToken)
		if err != nil {
			return fmt.Errorf("target auth: %w", err)
		}
		target, err := migrate.NewHTTPClient(targetURL, targetAuth, &http.Client{Timeout: 90 * time.Second})
		if err != nil {
			return err
		}
		dump, err := migrate.OpenSQLiteDump(dumpPath)
		if err != nil {
			return err
		}
		defer dump.Close()

		cmd.Printf("migration import starting: dump=%s syfon=%s batch=%d\n", dumpPath, targetURL, batchSize)
		stats, err := migrate.Import(context.Background(), dump, target, batchSize)
		if err != nil {
			return err
		}
		cmd.Printf("migration import complete: read=%d loaded=%d skipped=%d errors=%d\n",
			stats.Fetched,
			stats.Loaded,
			stats.Skipped,
			stats.Errors,
		)
		return nil
	},
}

func init() {
	Cmd.AddCommand(exportCmd)
	Cmd.AddCommand(importCmd)

	exportCmd.Flags().StringVar(&indexdURL, "indexd-url", "", "Source Gen3 or Indexd base URL; may be the Gen3 root or /index URL")
	exportCmd.Flags().StringVar(&dumpPath, "dump", "", "SQLite dump file to write")
	exportCmd.Flags().StringVar(&sourceProfile, "source-profile", "", "Gen3 profile for source reads from ~/.gen3/gen3_client_config.ini")
	exportCmd.Flags().StringVar(&sourceToken, "source-token", "", "Bearer token for source reads; overrides --source-profile")
	exportCmd.Flags().IntVar(&batchSize, "batch-size", 500, "Records to fetch per batch; capped at Indexd's 1024 max")
	exportCmd.Flags().IntVar(&limit, "limit", 0, "Maximum number of unique records to export; 0 means all records")
	exportCmd.Flags().IntVar(&sweeps, "sweeps", 1, "Full start-cursor sweeps to run; increase only for non-quiescent or inconsistent Indexd sources")
	exportCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Fetch, transform, and validate only; do not write the dump")
	exportCmd.Flags().StringArrayVar(&defaultAuthz, "default-authz", nil, "Default authz resource for source records with no authz; repeatable")

	importCmd.Flags().StringVar(&dumpPath, "dump", "", "SQLite dump file to read")
	importCmd.Flags().StringVar(&targetProfile, "target-profile", "", "Gen3 profile for target writes")
	importCmd.Flags().StringVar(&targetToken, "target-token", "", "Bearer token for target writes; overrides --target-profile")
	importCmd.Flags().IntVar(&batchSize, "batch-size", 500, "Records to load per /index/bulk request")
}

func targetServerURL(cmd *cobra.Command) (string, error) {
	flag := cmd.Root().PersistentFlags().Lookup("server")
	if flag == nil {
		return "", fmt.Errorf("target --server flag not found")
	}
	target := strings.TrimRight(strings.TrimSpace(flag.Value.String()), "/")
	if target == "" {
		return "", fmt.Errorf("--server cannot be empty")
	}
	return target, nil
}

func authFromInputs(profile, token string) (migrate.AuthConfig, error) {
	token = strings.TrimSpace(token)
	if token != "" {
		return migrate.AuthConfig{BearerToken: token}, nil
	}

	profile = strings.TrimSpace(profile)
	if profile == "" {
		return migrate.AuthConfig{}, nil
	}

	manager := conf.NewConfigure(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	credential, err := manager.Load(profile)
	if err != nil {
		return migrate.AuthConfig{}, err
	}
	if strings.TrimSpace(credential.AccessToken) != "" {
		return migrate.AuthConfig{BearerToken: credential.AccessToken}, nil
	}
	if strings.TrimSpace(credential.KeyID) != "" || strings.TrimSpace(credential.APIKey) != "" {
		return migrate.AuthConfig{Basic: &migrate.BasicAuth{Username: credential.KeyID, Password: credential.APIKey}}, nil
	}
	return migrate.AuthConfig{}, fmt.Errorf("profile %q has no access_token or key_id/api_key", profile)
}
