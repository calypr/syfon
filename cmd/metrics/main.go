package metricscmd

import (
	"encoding/json"
	"fmt"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/client/syfonclient"
	"github.com/spf13/cobra"
)

var (
	metricsOrganization string
	metricsProject      string
	metricsDirection    string
	metricsReconcile    string
	metricsFrom         string
	metricsTo           string
	metricsProvider     string
	metricsBucket       string
	metricsSHA256       string
	metricsUser         string
	metricsGroupBy      string
	metricsAllowStale   bool
	metricsSyncStatus   string
	metricsSyncLimit    int
	metricsImported     int64
	metricsMatched      int64
	metricsAmbiguous    int64
	metricsUnmatched    int64
	metricsError        string
	metricsToken        string
	metricsUsername     string
	metricsPassword     string
)

var Cmd = &cobra.Command{
	Use:   "metrics",
	Short: "Query Syfon metrics",
}

var transfersCmd = &cobra.Command{
	Use:   "transfers",
	Short: "Query transfer attribution metrics",
}

var transfersSummaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "Summarize transfer attribution metrics",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newMetricsClient(cmd)
		if err != nil {
			return err
		}
		summary, err := c.Metrics().TransferSummary(cmd.Context(), transferOptions())
		if err != nil {
			return err
		}
		return writeJSON(cmd, summary)
	},
}

var transfersBreakdownCmd = &cobra.Command{
	Use:   "breakdown",
	Short: "Group transfer attribution metrics",
	RunE: func(cmd *cobra.Command, args []string) error {
		groupBy := strings.TrimSpace(metricsGroupBy)
		switch groupBy {
		case "", "scope", "user", "provider", "object":
		default:
			return fmt.Errorf("unsupported group-by %q", groupBy)
		}
		c, err := newMetricsClient(cmd)
		if err != nil {
			return err
		}
		resp, err := c.Metrics().TransferBreakdown(cmd.Context(), transferOptions())
		if err != nil {
			return err
		}
		return writeJSON(cmd, resp)
	},
}

var transfersSyncCmd = &cobra.Command{
	Use:   "sync",
	Short: "Record provider transfer sync status",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newMetricsClient(cmd)
		if err != nil {
			return err
		}
		runs, err := c.Metrics().RecordProviderTransferSync(cmd.Context(), syncOptions())
		if err != nil {
			return err
		}
		return writeJSON(cmd, runs)
	},
}

var transfersSyncStatusCmd = &cobra.Command{
	Use:   "sync-status",
	Short: "List provider transfer sync status",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newMetricsClient(cmd)
		if err != nil {
			return err
		}
		runs, err := c.Metrics().ProviderTransferSyncStatus(cmd.Context(), syncOptions())
		if err != nil {
			return err
		}
		return writeJSON(cmd, runs)
	},
}

func init() {
	Cmd.AddCommand(transfersCmd)
	transfersCmd.AddCommand(transfersSummaryCmd)
	transfersCmd.AddCommand(transfersBreakdownCmd)
	transfersCmd.AddCommand(transfersSyncCmd)
	transfersCmd.AddCommand(transfersSyncStatusCmd)

	for _, c := range []*cobra.Command{transfersSummaryCmd, transfersBreakdownCmd, transfersSyncCmd, transfersSyncStatusCmd} {
		c.Flags().StringVar(&metricsOrganization, "organization", "", "Organization/program filter")
		c.Flags().StringVar(&metricsProject, "project", "", "Project filter")
		c.Flags().StringVar(&metricsFrom, "from", "", "RFC3339 start time filter")
		c.Flags().StringVar(&metricsTo, "to", "", "RFC3339 end time filter")
		c.Flags().StringVar(&metricsProvider, "provider", "", "Storage provider filter")
		c.Flags().StringVar(&metricsBucket, "bucket", "", "Bucket filter")
		c.Flags().StringVar(&metricsToken, "token", "", "Bearer token for Gen3-protected metrics")
		c.Flags().StringVar(&metricsUsername, "username", "", "Basic auth username")
		c.Flags().StringVar(&metricsPassword, "password", "", "Basic auth password")
	}
	for _, c := range []*cobra.Command{transfersSummaryCmd, transfersBreakdownCmd} {
		c.Flags().StringVar(&metricsDirection, "direction", "", "Transfer direction filter: download or upload")
		c.Flags().StringVar(&metricsReconcile, "reconciliation-status", "", "Provider reconciliation filter: matched, ambiguous, unmatched, or all")
		c.Flags().StringVar(&metricsSHA256, "sha256", "", "SHA256 filter")
		c.Flags().StringVar(&metricsUser, "user", "", "Actor email or subject filter")
		c.Flags().BoolVar(&metricsAllowStale, "allow-stale", false, "Deprecated: metrics always return persisted data with freshness metadata")
	}
	transfersBreakdownCmd.Flags().StringVar(&metricsGroupBy, "group-by", "scope", "Breakdown grouping: scope, user, provider, or object")
	transfersSyncCmd.Flags().StringVar(&metricsSyncStatus, "status", "pending", "Sync status to record: pending, completed, or failed")
	transfersSyncCmd.Flags().Int64Var(&metricsImported, "imported-events", 0, "Provider events imported in this sync window")
	transfersSyncCmd.Flags().Int64Var(&metricsMatched, "matched-events", 0, "Imported provider events matched to Syfon access grants")
	transfersSyncCmd.Flags().Int64Var(&metricsAmbiguous, "ambiguous-events", 0, "Imported provider events with ambiguous attribution")
	transfersSyncCmd.Flags().Int64Var(&metricsUnmatched, "unmatched-events", 0, "Imported provider events not matched to Syfon access grants")
	transfersSyncCmd.Flags().StringVar(&metricsError, "error-message", "", "Error message for failed sync status")
	transfersSyncStatusCmd.Flags().IntVar(&metricsSyncLimit, "limit", 100, "Maximum sync windows to return")
}

func newMetricsClient(cmd *cobra.Command) (syfonclient.SyfonClient, error) {
	serverURL, err := cmd.Flags().GetString("server")
	if err != nil {
		return nil, fmt.Errorf("get server flag: %w", err)
	}
	opts := []syclient.Option{}
	if token := strings.TrimSpace(metricsToken); token != "" {
		opts = append(opts, syclient.WithBearerToken(token))
	}
	if username := strings.TrimSpace(metricsUsername); username != "" {
		opts = append(opts, syclient.WithBasicAuth(username, metricsPassword))
	}
	return syclient.New(serverURL, opts...)
}

func transferOptions() syfonclient.TransferMetricsOptions {
	return syfonclient.TransferMetricsOptions{
		Organization:         strings.TrimSpace(metricsOrganization),
		ProjectID:            strings.TrimSpace(metricsProject),
		Direction:            strings.TrimSpace(metricsDirection),
		ReconciliationStatus: strings.TrimSpace(metricsReconcile),
		From:                 strings.TrimSpace(metricsFrom),
		To:                   strings.TrimSpace(metricsTo),
		Provider:             strings.TrimSpace(metricsProvider),
		Bucket:               strings.TrimSpace(metricsBucket),
		SHA256:               strings.TrimSpace(metricsSHA256),
		User:                 strings.TrimSpace(metricsUser),
		GroupBy:              strings.TrimSpace(metricsGroupBy),
		AllowStale:           metricsAllowStale,
	}
}

func syncOptions() syfonclient.ProviderTransferSyncOptions {
	return syfonclient.ProviderTransferSyncOptions{
		Organization:    strings.TrimSpace(metricsOrganization),
		ProjectID:       strings.TrimSpace(metricsProject),
		Provider:        strings.TrimSpace(metricsProvider),
		Bucket:          strings.TrimSpace(metricsBucket),
		From:            strings.TrimSpace(metricsFrom),
		To:              strings.TrimSpace(metricsTo),
		Status:          strings.TrimSpace(metricsSyncStatus),
		ImportedEvents:  metricsImported,
		MatchedEvents:   metricsMatched,
		AmbiguousEvents: metricsAmbiguous,
		UnmatchedEvents: metricsUnmatched,
		ErrorMessage:    strings.TrimSpace(metricsError),
		Limit:           metricsSyncLimit,
	}
}

func writeJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
