package ping

import (
	"fmt"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "ping",
	Short: "Check Syfon server health endpoint",
	RunE: func(cmd *cobra.Command, args []string) error {
		base := cliutil.NormalizedServerURL(cmd)
		if err := cliutil.NewSyfonClient(cmd).Ping(cmd.Context()); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Syfon is reachable at %s\n", base)
		return nil
	},
}
