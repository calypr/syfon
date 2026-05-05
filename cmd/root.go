package cmd

import (
	"os"
	"strings"

	"github.com/calypr/syfon/cmd/addurl"
	"github.com/calypr/syfon/cmd/bucket"
	"github.com/calypr/syfon/cmd/cliauth"
	"github.com/calypr/syfon/cmd/download"
	listcmd "github.com/calypr/syfon/cmd/list"
	metricscmd "github.com/calypr/syfon/cmd/metrics"
	migratecmd "github.com/calypr/syfon/cmd/migrate"
	"github.com/calypr/syfon/cmd/ping"
	"github.com/calypr/syfon/cmd/rm"
	"github.com/calypr/syfon/cmd/server"
	"github.com/calypr/syfon/cmd/sha256sum"
	"github.com/calypr/syfon/cmd/upload"
	"github.com/calypr/syfon/cmd/validate"
	"github.com/calypr/syfon/cmd/version"
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
	cliauth.RegisterRootFlags(RootCmd.PersistentFlags())

	RootCmd.AddCommand(validate.Cmd)
	RootCmd.AddCommand(server.Cmd)
	RootCmd.AddCommand(version.Cmd)
	RootCmd.AddCommand(ping.Cmd)
	RootCmd.AddCommand(bucket.Cmd)
	RootCmd.AddCommand(addurl.Cmd)
	RootCmd.AddCommand(upload.Cmd)
	RootCmd.AddCommand(download.Cmd)
	RootCmd.AddCommand(sha256sum.Cmd)
	RootCmd.AddCommand(listcmd.Cmd)
	RootCmd.AddCommand(rm.Cmd)
	RootCmd.AddCommand(metricscmd.Cmd)
	RootCmd.AddCommand(migratecmd.Cmd)
}
