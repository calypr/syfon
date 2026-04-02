package download

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var (
	downloadDid string
	downloadOut string
)

var Cmd = &cobra.Command{
	Use:   "download",
	Short: "Download a DRS object by DID",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if strings.TrimSpace(downloadDid) == "" {
			return fmt.Errorf("--did is required")
		}
		out := strings.TrimSpace(downloadOut)
		if out == "" {
			out = filepath.Base(downloadDid)
		}
		signed, err := cliutil.NewSyfonClient(cmd).GetDownloadURL(ctx, downloadDid)
		if err != nil {
			return err
		}
		if strings.TrimSpace(signed.URL) == "" {
			return fmt.Errorf("server returned empty download URL")
		}
		if err := cliutil.DownloadSignedURLToPath(ctx, signed.URL, out); err != nil {
			return err
		}
		fmt.Fprintf(cmd.OutOrStdout(), "downloaded %s -> %s\n", downloadDid, out)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&downloadDid, "did", "", "DRS object DID")
	Cmd.Flags().StringVar(&downloadOut, "out", "", "Output file path")
}
