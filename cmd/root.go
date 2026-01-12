package cmd

import (
	"github.com/calypr/drs-server/cmd/server"
	"github.com/calypr/drs-server/cmd/validate"
	"github.com/spf13/cobra"
)

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "drs-server",
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		//pre-run code here
	},
}

func init() {
	RootCmd.AddCommand(validate.Cmd)
	RootCmd.AddCommand(server.Cmd)
}
