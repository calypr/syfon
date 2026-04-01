package cmd

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/spf13/cobra"
)

var sha256Did string

var sha256sumCmd = &cobra.Command{
	Use:   "sha256sum",
	Short: "Download object to temp storage, compute sha256, update record, and print hash",
	RunE: func(cmd *cobra.Command, args []string) error {
		if strings.TrimSpace(sha256Did) == "" {
			return fmt.Errorf("--did is required")
		}

		var signed internalapi.InternalSignedURL
		if err := doJSON(http.MethodGet, "/data/download/"+url.PathEscape(sha256Did), nil, &signed); err != nil {
			return err
		}

		tmpFile, err := os.CreateTemp("", "syfon-sha256-*")
		if err != nil {
			return err
		}
		tmpPath := tmpFile.Name()
		tmpFile.Close()
		defer os.Remove(tmpPath)

		if err := downloadSignedURLToPath(signed.GetUrl(), tmpPath); err != nil {
			return err
		}
		data, err := os.ReadFile(tmpPath)
		if err != nil {
			return err
		}
		hash := sha256.Sum256(data)
		sum := hex.EncodeToString(hash[:])

		rec, err := getInternalRecord(sha256Did)
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
		if err := putInternalRecord(sha256Did, rec); err != nil {
			return err
		}

		fmt.Fprintln(cmd.OutOrStdout(), sum)
		return nil
	},
}

func init() {
	sha256sumCmd.Flags().StringVar(&sha256Did, "did", "", "DRS object DID")
}
