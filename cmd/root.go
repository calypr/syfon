package cmd

import (
	"os"
	"strings"

	"github.com/calypr/syfon/cmd/server"
	"github.com/calypr/syfon/cmd/validate"
	"github.com/spf13/cobra"
)

var serverBaseURL string

// RootCmd represents the root command
var RootCmd = &cobra.Command{
	Use:           "syfon",
	Aliases:       []string{"drs-servr"},
	SilenceErrors: true,
	SilenceUsage:  true,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		//pre-run code here
	},
}

func init() {
	defaultServerURL := strings.TrimSpace(os.Getenv("SYFON_SERVER_URL"))
	if defaultServerURL == "" {
		defaultServerURL = strings.TrimSpace(os.Getenv("DRS_SERVER_URL"))
	}
	if defaultServerURL == "" {
		defaultServerURL = "http://127.0.0.1:8080"
	}
	RootCmd.PersistentFlags().StringVar(&serverBaseURL, "server", defaultServerURL, "Syfon server base URL")

	RootCmd.AddCommand(validate.Cmd)
	RootCmd.AddCommand(server.Cmd)
	RootCmd.AddCommand(versionCmd)
	RootCmd.AddCommand(pingCmd)
	RootCmd.AddCommand(addURLCmd)
	RootCmd.AddCommand(uploadCmd)
	RootCmd.AddCommand(downloadCmd)
	RootCmd.AddCommand(sha256sumCmd)
}
