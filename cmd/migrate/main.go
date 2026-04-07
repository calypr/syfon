// Package migrateCmd provides the "migrate" subcommand, which runs an
// API-driven, idempotent ETL migration from an Indexd server to Syfon.
package migrateCmd

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/calypr/syfon/migrate"
	"github.com/spf13/cobra"
)

var (
	migrateIndexdURL  string
	migrateBatchSize  int
	migrateLimit      int
	migrateDryRun     bool
	migrateDefaultAuthz []string
)

// Cmd is the top-level "migrate" cobra command.
var Cmd = &cobra.Command{
	Use:   "migrate",
	Short: "Migrate records from an Indexd server to this Syfon instance",
	Long: `migrate runs an API-driven, idempotent ETL pipeline that:

  1. Extracts records from a source Indexd (or Syfon-compat) server via its
     paginated /index API.
  2. Transforms each record to the GA4GH DRS data model (issue #20):
       did → id, file_name → name, urls → access_methods,
       hashes → checksums, authz → authz
     Deprecated fields (baseid, rev, metadata, acl, form, uploader) are
     silently dropped.
  3. Validates checksums, URLs and authz are preserved.
  4. Loads objects into the target Syfon server via POST /index/bulk.

The pipeline is idempotent: re-running is safe.`,
	Example: `  # Dry-run: show what would be migrated without writing anything
  syfon migrate --indexd-url https://indexd.example.org --dry-run

  # Live run against a local Syfon server
  syfon migrate --indexd-url https://indexd.example.org --server http://localhost:8080

  # Migrate only the first 1000 records in batches of 200
  syfon migrate --indexd-url https://indexd.example.org --limit 1000 --batch-size 200

  # Apply a default authz resource to records that have none
  syfon migrate --indexd-url https://indexd.example.org --default-authz /programs/open`,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		slog.SetDefault(logger)

		indexdURL := strings.TrimSpace(migrateIndexdURL)
		if indexdURL == "" {
			return fmt.Errorf("--indexd-url is required")
		}
		syfonURL := cliutil.NormalizedServerURL(cmd)

		cfg := migrate.Config{
			IndexdURL:    indexdURL,
			SyfonURL:     syfonURL,
			BatchSize:    migrateBatchSize,
			Limit:        migrateLimit,
			DryRun:       migrateDryRun,
			DefaultAuthz: migrateDefaultAuthz,
		}

		if migrateDryRun {
			fmt.Fprintln(cmd.OutOrStdout(), "dry-run: no records will be written to Syfon")
		}
		fmt.Fprintf(cmd.OutOrStdout(), "migration starting: indexd=%s syfon=%s batch=%d limit=%d\n",
			indexdURL, syfonURL, migrateBatchSize, migrateLimit)

		stats, err := migrate.Run(cmd.Context(), cfg)
		if err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}

		fmt.Fprintf(cmd.OutOrStdout(), "migration complete: %s\n", stats)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&migrateIndexdURL, "indexd-url", "", "Source Indexd server base URL (required)")
	Cmd.Flags().IntVar(&migrateBatchSize, "batch-size", 100, "Number of records to fetch and write per batch")
	Cmd.Flags().IntVar(&migrateLimit, "limit", 0, "Maximum number of records to migrate (0 = all)")
	Cmd.Flags().BoolVar(&migrateDryRun, "dry-run", false, "Fetch and transform without writing to Syfon")
	Cmd.Flags().StringArrayVar(&migrateDefaultAuthz, "default-authz", nil,
		"Authz resource(s) applied to records with an empty authz list (repeatable)")
}

