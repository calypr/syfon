package rm

import (
	"fmt"
	"strings"

	syclient "github.com/calypr/syfon/client"
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
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
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
