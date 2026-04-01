package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var (
	addURLDid    string
	addURL       string
	addURLSize   int64
	addURLName   string
	addURLSHA256 string
)

var addURLCmd = &cobra.Command{
	Use:     "add-url",
	Aliases: []string{"addurl"},
	Short:   "Create or update a DRS record with an access URL",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(addURLDid) == "" {
			return fmt.Errorf("--did is required")
		}
		if strings.TrimSpace(addURL) == "" {
			return fmt.Errorf("--url is required")
		}
		if err := ensureRecordWithURL(addURLDid, addURL, addURLName, addURLSize, addURLSHA256); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "record updated: %s\n", addURLDid)
		return nil
	},
}

func init() {
	addURLCmd.Flags().StringVar(&addURLDid, "did", "", "DRS object DID")
	addURLCmd.Flags().StringVar(&addURL, "url", "", "Access URL to register")
	addURLCmd.Flags().Int64Var(&addURLSize, "size", 0, "Object size in bytes")
	addURLCmd.Flags().StringVar(&addURLName, "name", "", "Object file name")
	addURLCmd.Flags().StringVar(&addURLSHA256, "sha256", "", "Optional sha256 checksum")
}
