package cmd

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Check Syfon server health endpoint",
	RunE: func(cmd *cobra.Command, args []string) error {
		base := normalizedServerURL()
		resp, err := newHTTPClient().Get(base + "/healthz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("ping failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		fmt.Fprintf(cmd.OutOrStdout(), "Syfon is reachable at %s\n", base)
		return nil
	},
}
