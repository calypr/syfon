package bucket

import (
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/client/bucketapi"
	syclient "github.com/calypr/syfon/client"
	sybucket "github.com/calypr/syfon/client/bucket"
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

var Cmd = &cobra.Command{
	Use:   "bucket",
	Short: "Manage Syfon bucket credentials and scopes",
}

var addCmd = &cobra.Command{
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

		payload := bucketapi.PutBucketRequest{
			Bucket:       bucket,
			Organization: organization,
			ProjectId:    projectID,
		}
		payload.Provider = &provider
		if v := strings.TrimSpace(bucketRegion); v != "" {
			payload.Region = &v
		}
		if v := strings.TrimSpace(bucketAccessKey); v != "" {
			payload.AccessKey = &v
		}
		if v := strings.TrimSpace(bucketSecretKey); v != "" {
			payload.SecretKey = &v
		}
		if v := strings.TrimSpace(bucketEndpoint); v != "" {
			payload.Endpoint = &v
		}
		if v := strings.TrimSpace(bucketPath); v != "" {
			payload.Path = &v
		}

		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		// Validate bucket locally before pushing to server
		if err := sybucket.ValidateBucket(cmd.Context(), payload); err != nil {
			return fmt.Errorf("local bucket validation failed: %w", err)
		}

		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		if err := c.Buckets().Put(cmd.Context(), payload); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "bucket configured: %s (provider=%s org=%s project=%s)\n", bucket, provider, organization, projectID)
		return nil
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List configured buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		resp, err := c.Buckets().List(cmd.Context())
		if err != nil {
			return err
		}
		buckets := resp.S3BUCKETS
		if len(buckets) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "no buckets configured")
			return nil
		}
		names := make([]string, 0, len(buckets))
		for name := range buckets {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			md := buckets[name]
			provider := ""
			if md.Provider != nil {
				provider = *md.Provider
			}
			region := ""
			if md.Region != nil {
				region = *md.Region
			}
			programs := []string{}
			if md.Programs != nil {
				programs = *md.Programs
			}
			fmt.Fprintf(
				cmd.OutOrStdout(),
				"%s\tprovider=%s\tregion=%s\tprograms=%s\n",
				name,
				strings.TrimSpace(provider),
				strings.TrimSpace(region),
				strings.Join(programs, ","),
			)
		}
		return nil
	},
}

var removeCmd = &cobra.Command{
	Use:     "remove <bucket>",
	Aliases: []string{"rm", "delete"},
	Short:   "Remove bucket credentials and scopes",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		bucket := strings.TrimSpace(args[0])
		if bucket == "" {
			return fmt.Errorf("bucket is required")
		}
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		if err := c.Buckets().Delete(cmd.Context(), bucket); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "bucket removed: %s\n", bucket)
		return nil
	},
}

func init() {
	addCmd.Flags().StringVar(&bucketProvider, "provider", "s3", "Bucket provider: s3|gcs|azure")
	addCmd.Flags().StringVar(&bucketRegion, "region", "us-east-1", "Bucket region")
	addCmd.Flags().StringVar(&bucketAccessKey, "access-key", "", "S3 access key (required for new s3 creds)")
	addCmd.Flags().StringVar(&bucketSecretKey, "secret-key", "", "S3 secret key (required for new s3 creds)")
	addCmd.Flags().StringVar(&bucketEndpoint, "endpoint", "", "Custom endpoint URL")
	addCmd.Flags().StringVar(&bucketOrganization, "organization", "syfon", "Scope organization")
	addCmd.Flags().StringVar(&bucketProjectID, "project-id", "e2e", "Scope project id")
	addCmd.Flags().StringVar(&bucketPath, "path", "", "Optional bucket path prefix for this scope")

	Cmd.AddCommand(addCmd)
	Cmd.AddCommand(listCmd)
	Cmd.AddCommand(removeCmd)
}
