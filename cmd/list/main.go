package listcmd

import (
	"fmt"
	"sort"
	"strings"

	syclient "github.com/calypr/syfon/client"
	"github.com/calypr/syfon/cmd/cliutil"
	"github.com/spf13/cobra"
)

var (
	listLimit        int
	listPage         int
	listOrganization string
	listProject      string
)

var Cmd = &cobra.Command{
	Use:     "ls",
	Aliases: []string{"list"},
	Short:   "List indexed files",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := cliutil.NewSyfonClient(cmd)
		resp, err := c.Index().List(cmd.Context(), syclient.ListRecordsOptions{
			Limit:        listLimit,
			Page:         listPage,
			Organization: strings.TrimSpace(listOrganization),
			ProjectID:    strings.TrimSpace(listProject),
		})
		if err != nil {
			return err
		}

		records := resp.GetRecords()
		if len(records) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "no records found")
			return nil
		}

		// Keep output simple and script-friendly: DID<TAB>name<TAB>size.
		sort.Slice(records, func(i, j int) bool {
			return strings.TrimSpace(records[i].GetDid()) < strings.TrimSpace(records[j].GetDid())
		})
		for _, rec := range records {
			did := strings.TrimSpace(rec.GetDid())
			name := strings.TrimSpace(rec.GetFileName())
			if name == "" {
				name = "-"
			}
			size := rec.GetSize()
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%d\n", did, name, size)
		}
		return nil
	},
}

func init() {
	Cmd.Flags().IntVar(&listLimit, "limit", 100, "Maximum number of records to return")
	Cmd.Flags().IntVar(&listPage, "page", 0, "Page number for pagination")
	Cmd.Flags().StringVar(&listOrganization, "organization", "", "Optional organization/program filter")
	Cmd.Flags().StringVar(&listProject, "project", "", "Optional project filter")
}
