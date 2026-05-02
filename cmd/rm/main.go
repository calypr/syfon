package rm

import (
	"fmt"
	"strings"

	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/spf13/cobra"
)

var rmDID string

var Cmd = &cobra.Command{
	Use:     "rm",
	Aliases: []string{"delete"},
	Short:   "Remove a record by DID",
	RunE: func(cmd *cobra.Command, args []string) error {
		did := strings.TrimSpace(rmDID)
		if did == "" {
			return fmt.Errorf("--did is required")
		}
		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}
		if err := c.Index().Delete(cmd.Context(), did); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "removed %s\n", did)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&rmDID, "did", "", "DRS object DID")
}
