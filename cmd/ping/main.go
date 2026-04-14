package ping

import (
	"fmt"

	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "ping",
	Short: "Check Syfon server health endpoint",
	RunE: func(cmd *cobra.Command, args []string) error {
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
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
