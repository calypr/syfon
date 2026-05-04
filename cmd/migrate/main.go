package migratecmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	conf "github.com/calypr/syfon/client/config"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/migrate"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "migrate",
	Short: "Export and import Gen3 Indexd records for Syfon migration",
	Long:  "Export records from a Gen3-mounted Indexd API into a local SQLite dump, then import that dump into Syfon using the controlled_access-aware /index/bulk loader.",
}

var (
	dumpPath            string
	sourceProfile       string
	targetProfile       string
	sourceToken         string
	targetToken         string
	targetBasicUser     string
	targetBasicPassword string
	batchSize           int
	limit               int
	sweeps              int
	dryRun              bool
	skipAuthPreflight   bool
	defaultAuthz        []string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export records from Gen3 Indexd into a SQLite dump",
	RunE: func(cmd *cobra.Command, args []string) error {
		sourceURL, err := indexdURLFromServer(cmd)
		if err != nil {
			return err
		}
		dumpPath = normalizedDumpPath(dumpPath)

		sourceAuth, err := authFromInputs(sourceProfile, sourceToken)
		if err != nil {
			return fmt.Errorf("source auth: %w", err)
		}
		httpClient := migrationHTTPClient()
		source, err := migrate.NewHTTPClient(sourceURL, sourceAuth, httpClient)
		if err != nil {
			return err
		}
		var dump *migrate.SQLiteDump
		if !dryRun {
			dump, err = migrate.OpenSQLiteDump(dumpPath)
			if err != nil {
				return err
			}
			defer dump.Close()
		}

		if dryRun {
			cmd.Println("dry-run: no records will be written to the dump")
		}
		displayDump := dumpPath
		if dryRun {
			displayDump = "<dry-run: not written>"
		}
		cmd.Printf("migration export starting: indexd=%s dump=%s batch=%d limit=%d sweeps=%d\n", sourceURL, displayDump, batchSize, limit, sweeps)
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
		count := 0
		if dump != nil {
			var countErr error
			count, countErr = dump.Count(context.Background())
			if countErr != nil {
				return countErr
			}
		}
		if dryRun {
			cmd.Printf("migration export dry-run complete: fetched=%d transformed=%d valid=%d skipped=%d errors=%d unique_ids=%d dump_records=0\n",
				stats.Fetched,
				stats.Transformed,
				stats.Loaded,
				stats.Skipped,
				stats.Errors,
				stats.CountOfUniqueIDs,
			)
			return nil
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
		targetAuth, err := targetAuthFromInputs(targetProfile, targetToken, targetBasicUser, targetBasicPassword)
		if err != nil {
			return fmt.Errorf("target auth: %w", err)
		}
		target, err := migrate.NewHTTPClient(targetURL, targetAuth, migrationHTTPClient())
		if err != nil {
			return err
		}
		dump, err := migrate.OpenExistingSQLiteDump(dumpPath)
		if err != nil {
			return err
		}
		defer dump.Close()

		cmd.Printf("migration import starting: dump=%s syfon=%s batch=%d\n", dumpPath, targetURL, batchSize)
		if !skipAuthPreflight && strings.TrimSpace(targetAuth.BearerToken) != "" {
			cmd.Println("migration import preflight: checking target create privileges")
			report, err := migrate.PreflightImport(context.Background(), dump, target, batchSize)
			if err != nil {
				var preflightErr *migrate.ImportPreflightError
				if errors.As(err, &preflightErr) {
					printImportPreflightFailure(cmd, preflightErr.Report)
					return fmt.Errorf("target authorization preflight failed: missing create access for %d/%d records across %d scopes; copy/paste scope list printed above",
						preflightErr.Report.MissingRecords,
						preflightErr.Report.Records,
						len(preflightErr.Report.MissingResources),
					)
				}
				return fmt.Errorf("target authorization preflight failed: %w", err)
			}
			cmd.Printf("migration import preflight complete: records=%d required_scopes=%d missing_scopes=0\n",
				report.Records,
				len(report.RequiredResources),
			)
		}
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

func printImportPreflightFailure(cmd *cobra.Command, report migrate.ImportPreflightReport) {
	scopes := migrate.FormatPreflightScopes(report.MissingResources)
	cmd.Printf("migration import preflight failed: missing create access for %d/%d records across %d scopes\n",
		report.MissingRecords,
		report.Records,
		len(scopes),
	)
	if report.FirstDeniedRecord != "" {
		cmd.Printf("first denied record: %s\n", report.FirstDeniedRecord)
	}
	cmd.Println()
	cmd.Println("Copy/paste scope list:")
	cmd.Println(strings.Join(scopes, ", "))
	cmd.Println()
}

func init() {
	Cmd.AddCommand(exportCmd)
	Cmd.AddCommand(importCmd)

	exportCmd.Flags().StringVar(&dumpPath, "dump", "", "SQLite dump file to write (default ./indexd-records.sqlite)")
	exportCmd.Flags().StringVar(&sourceProfile, "source-profile", "", "Gen3 profile for source reads from ~/.gen3/gen3_client_config.ini")
	exportCmd.Flags().StringVar(&sourceToken, "source-token", "", "Bearer token for source reads; overrides --source-profile")
	exportCmd.Flags().IntVar(&batchSize, "batch-size", 500, "Records to fetch per batch; capped at Indexd's 1024 max")
	exportCmd.Flags().IntVar(&limit, "limit", 0, "Maximum number of unique records to export; 0 means all records")
	exportCmd.Flags().IntVar(&sweeps, "sweeps", 1, "Full start-cursor sweeps to run; increase only for non-quiescent or inconsistent Indexd sources")
	exportCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Fetch, transform, and validate only; do not write the dump")
	exportCmd.Flags().StringArrayVar(&defaultAuthz, "default-controlled-access", nil, "Default controlled_access resource for source records with no authz; repeatable")
	exportCmd.Flags().StringArrayVar(&defaultAuthz, "default-authz", nil, "Deprecated alias for --default-controlled-access")

	importCmd.Flags().StringVar(&dumpPath, "dump", "", "SQLite dump file to read")
	importCmd.Flags().StringVar(&targetProfile, "target-profile", "", "Gen3 profile for target writes")
	importCmd.Flags().StringVar(&targetToken, "target-token", "", "Bearer token for target writes; overrides --target-profile")
	importCmd.Flags().StringVar(&targetBasicUser, "target-basic-user", "", "Basic auth username for local target Syfon writes")
	importCmd.Flags().StringVar(&targetBasicPassword, "target-basic-password", "", "Basic auth password for local target Syfon writes")
	importCmd.Flags().IntVar(&batchSize, "batch-size", 500, "Records to load per controlled_access-aware /index/bulk request")
	importCmd.Flags().BoolVar(&skipAuthPreflight, "skip-auth-preflight", false, "Skip target create-privilege preflight before importing")
}

func indexdURLFromServer(cmd *cobra.Command) (string, error) {
	server, err := targetServerURL(cmd)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(server, "/") + "/index/index", nil
}

func normalizedDumpPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "indexd-records.sqlite"
	}
	return path
}

func migrationHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = 60 * time.Second
	transport.ResponseHeaderTimeout = 90 * time.Second
	return &http.Client{
		Timeout:   3 * time.Minute,
		Transport: transport,
	}
}

func targetServerURL(cmd *cobra.Command) (string, error) {
	return cliauth.ResolveServerURL(cmd)
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

func targetAuthFromInputs(profile, token, basicUser, basicPassword string) (migrate.AuthConfig, error) {
	basicUser = strings.TrimSpace(basicUser)
	basicPassword = strings.TrimSpace(basicPassword)
	if basicUser != "" || basicPassword != "" {
		if basicUser == "" || basicPassword == "" {
			return migrate.AuthConfig{}, fmt.Errorf("--target-basic-user and --target-basic-password must be set together")
		}
		if strings.TrimSpace(token) != "" {
			return migrate.AuthConfig{}, fmt.Errorf("--target-token cannot be combined with --target-basic-user/--target-basic-password")
		}
		if strings.TrimSpace(profile) != "" {
			return migrate.AuthConfig{}, fmt.Errorf("--target-profile cannot be combined with --target-basic-user/--target-basic-password")
		}
		return migrate.AuthConfig{Basic: &migrate.BasicAuth{Username: basicUser, Password: basicPassword}}, nil
	}
	return authFromInputs(profile, token)
}
