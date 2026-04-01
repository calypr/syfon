package cmd

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var (
	bucketProvider     string
	bucketRegion       string
	bucketAccessKey    string
	bucketSecretKey    string
	bucketEndpoint     string
	bucketOrganization string
	bucketProjectID    string
	bucketPath         string
)

var bucketCmd = &cobra.Command{
	Use:   "bucket",
	Short: "Manage Syfon bucket credentials and scopes",
}

var bucketAddCmd = &cobra.Command{
	Use:   "add <bucket>",
	Short: "Create or update bucket credentials/scope on the server",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		bucket := strings.TrimSpace(args[0])
		if bucket == "" {
			return fmt.Errorf("bucket is required")
		}

		provider := strings.TrimSpace(bucketProvider)
		if provider == "" {
			provider = "s3"
		}
		organization := strings.TrimSpace(bucketOrganization)
		projectID := strings.TrimSpace(bucketProjectID)
		if organization == "" || projectID == "" {
			return fmt.Errorf("--organization and --project-id are required")
		}

		payload := map[string]string{
			"bucket":       bucket,
			"provider":     provider,
			"region":       strings.TrimSpace(bucketRegion),
			"organization": organization,
			"project_id":   projectID,
		}
		if v := strings.TrimSpace(bucketAccessKey); v != "" {
			payload["access_key"] = v
		}
		if v := strings.TrimSpace(bucketSecretKey); v != "" {
			payload["secret_key"] = v
		}
		if v := strings.TrimSpace(bucketEndpoint); v != "" {
			payload["endpoint"] = v
		}
		if v := strings.TrimSpace(bucketPath); v != "" {
			payload["path"] = v
		}

		if err := doJSON(http.MethodPut, "/data/buckets", payload, nil); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "bucket configured: %s (provider=%s org=%s project=%s)\n", bucket, provider, organization, projectID)
		return nil
	},
}

func init() {
	bucketAddCmd.Flags().StringVar(&bucketProvider, "provider", "s3", "Bucket provider: s3|gcs|azure|file")
	bucketAddCmd.Flags().StringVar(&bucketRegion, "region", "us-east-1", "Bucket region")
	bucketAddCmd.Flags().StringVar(&bucketAccessKey, "access-key", "", "S3 access key (required for new s3 creds)")
	bucketAddCmd.Flags().StringVar(&bucketSecretKey, "secret-key", "", "S3 secret key (required for new s3 creds)")
	bucketAddCmd.Flags().StringVar(&bucketEndpoint, "endpoint", "", "Custom endpoint URL")
	bucketAddCmd.Flags().StringVar(&bucketOrganization, "organization", "syfon", "Scope organization")
	bucketAddCmd.Flags().StringVar(&bucketProjectID, "project-id", "e2e", "Scope project id")
	bucketAddCmd.Flags().StringVar(&bucketPath, "path", "", "Optional bucket path prefix for this scope")

	bucketCmd.AddCommand(bucketAddCmd)
}

