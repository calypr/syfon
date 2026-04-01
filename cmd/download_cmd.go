package cmd

import (
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/spf13/cobra"
)

var (
	downloadDid string
	downloadOut string
)

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a DRS object by DID",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(downloadDid) == "" {
			return fmt.Errorf("--did is required")
		}
		out := strings.TrimSpace(downloadOut)
		if out == "" {
			out = filepath.Base(downloadDid)
		}
		var signed internalapi.InternalSignedURL
		if err := doJSON(http.MethodGet, "/data/download/"+url.PathEscape(downloadDid), nil, &signed); err != nil {
			return err
		}
		if strings.TrimSpace(signed.GetUrl()) == "" {
			return fmt.Errorf("server returned empty download URL")
		}
		if err := downloadSignedURLToPath(signed.GetUrl(), out); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", downloadDid, out)
		return nil
	},
}

func init() {
	downloadCmd.Flags().StringVar(&downloadDid, "did", "", "DRS object DID")
	downloadCmd.Flags().StringVar(&downloadOut, "out", "", "Output file path")
}
