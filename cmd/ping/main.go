package ping

import (
	"fmt"

	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "ping",
	Short: "Check Syfon server health endpoint",
	RunE: func(cmd *cobra.Command, args []string) error {
		serverURL, err := cliauth.ResolveServerURL(cmd)
		if err != nil {
			return err
		}
		c, err := cliauth.NewServerClient(cmd)
		if err != nil {
			return err
		}
		if err := c.Health().Ping(cmd.Context()); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Syfon is reachable at %s\n", serverURL)
		return nil
	},
}
