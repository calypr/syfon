package sha256sum

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var sha256Did string

var Cmd = &cobra.Command{
	Use:   "sha256sum",
	Short: "Download object to temp storage, compute sha256, update record, and print hash",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		if strings.TrimSpace(sha256Did) == "" {
			return fmt.Errorf("--did is required")
		}

		c := cliutil.NewSyfonClient(cmd)
		signed, err := c.GetDownloadURL(ctx, sha256Did)
		if err != nil {
			return err
		}

		tmpFile, err := os.CreateTemp("", "syfon-sha256-*")
		if err != nil {
			return err
		}
		tmpPath := tmpFile.Name()
		tmpFile.Close()
		defer os.Remove(tmpPath)

		if err := cliutil.DownloadSignedURLToPath(ctx, signed.URL, tmpPath); err != nil {
			return err
		}
		data, err := os.ReadFile(tmpPath)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		sum := hex.EncodeToString(hash[:])

		rec, err := cliutil.GetInternalRecord(ctx, c, sha256Did)
		if err != nil {
			return err
		}
		hashes := rec.GetHashes()
		if hashes == nil {
			hashes = map[string]string{}
		}
		hashes["sha256"] = sum
		rec.SetHashes(hashes)
		if strings.TrimSpace(rec.GetDid()) == "" {
			rec.SetDid(sha256Did)
		}
		if err := cliutil.PutInternalRecord(ctx, c, sha256Did, rec); err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), sum)
		return nil
	},
}

func init() {
	Cmd.Flags().StringVar(&sha256Did, "did", "", "DRS object DID")
}
