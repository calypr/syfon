package deleteproject

import (
	"fmt"
	"strings"

	"github.com/calypr/syfon/cmd/cliauth"
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
		scope, err := parseScopeArg(args[0])
		if err != nil {
			return err
		}

		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}

		resp, err := c.Buckets().DeleteProjectData(cmd.Context(), scope.org, scope.project)
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

type projectScope struct {
	org     string
	project string
}

func parseScopeArg(raw string) (projectScope, error) {
	parts := strings.Split(strings.TrimSpace(raw), "/")
	if len(parts) != 2 {
		return projectScope{}, fmt.Errorf("invalid project scope %q: must be in format <organization>/<project-id>", raw)
	}
	scope := projectScope{
		org:     strings.TrimSpace(parts[0]),
		project: strings.TrimSpace(parts[1]),
	}
	if scope.org == "" || scope.project == "" {
		return projectScope{}, fmt.Errorf("project scope must be non-empty and in <organization>/<project-id> format")
	}
	return scope, nil
}
