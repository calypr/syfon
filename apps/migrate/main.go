package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/calypr/syfon/migrate"
)

var version = "dev"

var migrationRunner = migrate.Run

func main() {
	if err := runWithArgs(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	return runWithArgs(os.Args[1:], os.Stdout, os.Stderr)
}

func runWithArgs(args []string, stdout, stderr io.Writer) error {
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	var (
		indexdURL string
		syfonURL  string
		batchSize int
		limit     int
		dryRun    bool
		authzCSV  string
		showVer   bool
	)

	defaultServerURL := strings.TrimSpace(os.Getenv("SYFON_SERVER_URL"))
	if defaultServerURL == "" {
		defaultServerURL = strings.TrimSpace(os.Getenv("DRS_SERVER_URL"))
	}
	if defaultServerURL == "" {
		defaultServerURL = "http://127.0.0.1:8080"
	}

	fs := flag.NewFlagSet("syfon-migrate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&indexdURL, "indexd-url", "", "Source Indexd server base URL (required)")
	fs.StringVar(&syfonURL, "server", defaultServerURL, "Target Syfon server base URL")
	fs.IntVar(&batchSize, "batch-size", 100, "Number of records to fetch/write per batch")
	fs.IntVar(&limit, "limit", 0, "Maximum number of records to migrate (0 = all)")
	fs.BoolVar(&dryRun, "dry-run", false, "Fetch and transform only; do not write to Syfon")
	fs.StringVar(&authzCSV, "default-authz", "", "Comma-separated default authz resources for records with empty authz")
	fs.BoolVar(&showVer, "version", false, "Print binary version and exit")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if showVer {
		fmt.Fprintln(stdout, version)
		return nil
	}

	indexdURL = strings.TrimSpace(indexdURL)
	if indexdURL == "" {
		return fmt.Errorf("--indexd-url is required")
	}
	syfonURL = strings.TrimRight(strings.TrimSpace(syfonURL), "/")
	if syfonURL == "" {
		return fmt.Errorf("--server cannot be empty")
	}

	defaultAuthz := splitCSV(authzCSV)
	cfg := migrate.Config{
		IndexdURL:    indexdURL,
		SyfonURL:     syfonURL,
		BatchSize:    batchSize,
		Limit:        limit,
		DryRun:       dryRun,
		DefaultAuthz: defaultAuthz,
	}

	if dryRun {
		fmt.Fprintln(stdout, "dry-run: no records will be written to Syfon")
	}
	fmt.Fprintf(stdout, "migration starting: indexd=%s syfon=%s batch=%d limit=%d\n", indexdURL, syfonURL, batchSize, limit)

	stats, err := migrationRunner(context.Background(), cfg)
	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	fmt.Fprintf(stdout, "migration complete: %s\n", stats)
	return nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
