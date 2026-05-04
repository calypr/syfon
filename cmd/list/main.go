package listcmd

import (
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/calypr/syfon/apigen/client/internalapi"
	syfonclient "github.com/calypr/syfon/client/services"
	"github.com/calypr/syfon/client/transfer/upload"
	"github.com/calypr/syfon/cmd/cliauth"
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
		c, err := cliauth.NewServerClient(cmd)
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

		sort.Slice(records, func(i, j int) bool {
			return strings.TrimSpace(records[i].Did) < strings.TrimSpace(records[j].Did)
		})
		tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tORG\tPROJECT\tSIZE\tDID")
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
			org := ""
			if rec.Organization != nil {
				org = strings.TrimSpace(*rec.Organization)
			}
			project := ""
			if rec.Project != nil {
				project = strings.TrimSpace(*rec.Project)
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", name, org, project, upload.FormatSize(size), did)
		}
		return tw.Flush()
	},
}

func init() {
	Cmd.Flags().IntVar(&listLimit, "limit", 100, "Maximum number of records to return")
	Cmd.Flags().IntVar(&listPage, "page", 0, "Page number for pagination")
	Cmd.Flags().StringVar(&listOrganization, "organization", "", "Optional organization/program filter")
	Cmd.Flags().StringVar(&listProject, "project", "", "Optional project filter")
}
