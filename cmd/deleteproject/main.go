package deleteproject

import (
	"fmt"

	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/cmd/projectcopy"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "delete-project <organization>/<project-id>",
	Short: "Delete all Syfon records and project bucket mappings for a scope",
	Long: `Delete all Syfon records for an exact project scope.

The argument must be written as <organization>/<project-id>.

This command also removes any project-level bucket scope mappings for that exact scope.`,
	Example: `  syfon delete-project Ellrott_Lab/embedding_rotation`,
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		scope, err := projectcopy.ParseScopeArg(args[0], "project")
		if err != nil {
			return err
		}

		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}

		resp, err := c.Buckets().DeleteProjectData(cmd.Context(), scope.Organization, scope.Project)
		if err != nil {
			return err
		}

		fmt.Fprintf(
			cmd.OutOrStdout(),
			"deleted project %s/%s: %d records removed, %d project bucket mappings removed\n",
			resp.Organization,
			resp.ProjectId,
			resp.DeletedObjects,
			resp.DeletedBucketScopes,
		)
		return nil
	},
}
