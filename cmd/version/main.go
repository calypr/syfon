package version

import (
	"fmt"

	"github.com/calypr/syfon/version"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "version",
	Short: "Print Syfon version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Fprintf(cmd.OutOrStdout(), version.String())
	},
}
