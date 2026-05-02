package metricscmd

import (
	"encoding/json"
	"fmt"
	"strings"

	syclient "github.com/calypr/syfon/client"
	syfonclient "github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/internal/models"
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

type transferBillingReport struct {
	Summary          models.TransferAttributionSummary     `json:"summary"`
	StorageLocations []models.TransferAttributionBreakdown `json:"storage_locations"`
	Files            []models.TransferAttributionBreakdown `json:"files"`
}

var transfersBillingCmd = &cobra.Command{
	Use:   "billing",
	Short: "Report transfer billing totals by storage location and file",
	RunE: func(cmd *cobra.Command, args []string) error {
		c, err := newMetricsClient(cmd)
		if err != nil {
			return err
		}
		opts := transferOptions()
		summary, err := c.Metrics().TransferSummary(cmd.Context(), opts)
		if err != nil {
			return err
		}
		providerOpts := opts
		providerOpts.GroupBy = "provider"
		storageLocations, err := c.Metrics().TransferBreakdown(cmd.Context(), providerOpts)
		if err != nil {
			return err
		}
		objectOpts := opts
		objectOpts.GroupBy = "object"
		files, err := c.Metrics().TransferBreakdown(cmd.Context(), objectOpts)
		if err != nil {
			return err
		}
		return writeJSON(cmd, transferBillingReport{
			Summary:          summary,
			StorageLocations: storageLocations.Data,
			Files:            files.Data,
		})
	},
}

func init() {
	Cmd.AddCommand(transfersCmd)
	transfersCmd.AddCommand(transfersSummaryCmd)
	transfersCmd.AddCommand(transfersBreakdownCmd)
	transfersCmd.AddCommand(transfersBillingCmd)

	for _, c := range []*cobra.Command{transfersSummaryCmd, transfersBreakdownCmd, transfersBillingCmd} {
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
	for _, c := range []*cobra.Command{transfersSummaryCmd, transfersBreakdownCmd, transfersBillingCmd} {
		c.Flags().StringVar(&metricsDirection, "direction", "", "Transfer direction filter: download or upload")
		c.Flags().StringVar(&metricsReconcile, "reconciliation-status", "", "Provider reconciliation filter: matched, ambiguous, unmatched, or all")
		c.Flags().StringVar(&metricsSHA256, "sha256", "", "SHA256 filter")
		c.Flags().StringVar(&metricsUser, "user", "", "Actor email or subject filter")
		c.Flags().BoolVar(&metricsAllowStale, "allow-stale", false, "Deprecated: metrics always return persisted data with freshness metadata")
	}
	transfersBreakdownCmd.Flags().StringVar(&metricsGroupBy, "group-by", "scope", "Breakdown grouping: scope, user, provider, or object")
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

func writeJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
