package version

import (
	"fmt"

	"github.com/spf13/cobra"
)

func New(version string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print Syfon version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "Syfon %s\n", version)
		},
	}
}
