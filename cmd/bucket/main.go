package bucket

import (
	"fmt"
	"sort"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/cmd/cliutil"
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

		payload := syclient.PutBucketRequest{
			Bucket:       bucket,
			Organization: organization,
			ProjectId:    projectID,
		}
		payload.SetProvider(provider)
		if v := strings.TrimSpace(bucketRegion); v != "" {
			payload.SetRegion(v)
		}
		if v := strings.TrimSpace(bucketAccessKey); v != "" {
			payload.SetAccessKey(v)
		}
		if v := strings.TrimSpace(bucketSecretKey); v != "" {
			payload.SetSecretKey(v)
		}
		if v := strings.TrimSpace(bucketEndpoint); v != "" {
			payload.SetEndpoint(v)
		}
		if v := strings.TrimSpace(bucketPath); v != "" {
			payload.SetPath(v)
		}

		if err := cliutil.NewSyfonClient(cmd).PutBucket(cmd.Context(), payload); err != nil {
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
		resp, err := cliutil.NewSyfonClient(cmd).Buckets().List(cmd.Context())
		if err != nil {
			return err
		}
		buckets := resp.GetS3BUCKETS()
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
			fmt.Fprintf(
				cmd.OutOrStdout(),
				"%s\tprovider=%s\tregion=%s\tprograms=%s\n",
				name,
				strings.TrimSpace(md.GetProvider()),
				strings.TrimSpace(md.GetRegion()),
				strings.Join(md.GetPrograms(), ","),
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
		if err := cliutil.NewSyfonClient(cmd).Buckets().Delete(cmd.Context(), bucket); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "bucket removed: %s\n", bucket)
		return nil
	},
}

func init() {
	addCmd.Flags().StringVar(&bucketProvider, "provider", "s3", "Bucket provider: s3|gcs|azure|file")
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
