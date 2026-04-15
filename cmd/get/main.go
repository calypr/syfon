package get

import (
	"encoding/json"
	"fmt"

	syclient "github.com/calypr/syfon/client"
	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "get",
	Short: "Get DRS record",
	RunE: func(cmd *cobra.Command, args []string) error {
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}

		for _, i := range args {
			record, err := c.Index().Get(cmd.Context(), i)
			if err == nil {
				txt, err := json.Marshal(record)
				if err == nil {
					fmt.Printf("%s\n", txt)
				}
			} else {
				fmt.Printf("Error getting record for %s: %v\n", i, err)
			}
		}

		return nil
	},
}

func init() {

}
