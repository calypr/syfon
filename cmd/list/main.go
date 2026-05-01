package listcmd

import (
	"fmt"
	"sort"
	"strings"

	"github.com/calypr/syfon/apigen/client/internalapi"
	syclient "github.com/calypr/syfon/client"
	syfonclient "github.com/calypr/syfon/client/services"
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
		serverURL, err := cmd.Flags().GetString("server")
		if err != nil {
			return fmt.Errorf("get server flag: %w", err)
		}
		c, err := syclient.New(serverURL)
		if err != nil {
			return err
		}
		resp, err := c.Index().List(cmd.Context(), syfonclient.ListRecordsOptions{
			Limit:        listLimit,
			Page:         listPage,
			Organization: strings.TrimSpace(listOrganization),
			ProjectID:    strings.TrimSpace(listProject),
		})
		if err != nil {
			return err
		}

		var records []internalapi.InternalRecord
		if resp.Records != nil {
			records = *resp.Records
		}
		if len(records) == 0 {
			fmt.Fprintln(cmd.OutOrStdout(), "no records found")
			return nil
		}

		// Keep output simple and script-friendly: DID<TAB>name<TAB>size.
		sort.Slice(records, func(i, j int) bool {
			return strings.TrimSpace(records[i].Did) < strings.TrimSpace(records[j].Did)
		})
		for _, rec := range records {
			did := strings.TrimSpace(rec.Did)
			name := "-"
			if rec.FileName != nil {
				name = strings.TrimSpace(*rec.FileName)
			}
			size := int64(0)
			if rec.Size != nil {
				size = *rec.Size
			}
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
