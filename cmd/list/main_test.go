package listcmd

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/client/internalapi"
	syfonclient "github.com/calypr/syfon/client/services"
)

type fakeIndexLister struct {
	responses map[string]internalapi.ListRecordsResponse
	calls     []syfonclient.ListRecordsOptions
}

func (f *fakeIndexLister) List(_ context.Context, opts syfonclient.ListRecordsOptions) (internalapi.ListRecordsResponse, error) {
	f.calls = append(f.calls, opts)
	key := fmt.Sprintf("%s|%s|%s|%d|%s", opts.Organization, opts.ProjectID, opts.Path, opts.Limit, opts.Start)
	if resp, ok := f.responses[key]; ok {
		return resp, nil
	}
	return internalapi.ListRecordsResponse{}, fmt.Errorf("unexpected list call: %s", key)
}

func TestListRecordsRecursiveWalksDirectories(t *testing.T) {
	rootRecords := []internalapi.InternalRecord{
		{Did: "did-root", FileName: ptr("README.md")},
	}
	rootDirs := []internalapi.IndexDirectory{
		{Name: "data", Path: "data"},
		{Name: "nested", Path: "nested"},
	}
	dataRecords := []internalapi.InternalRecord{
		{Did: "did-data", FileName: ptr("data/file1.txt")},
	}
	dataDirs := []internalapi.IndexDirectory{
		{Name: "more", Path: "data/more"},
	}
	moreRecords := []internalapi.InternalRecord{
		{Did: "did-more", FileName: ptr("data/more/file2.txt")},
	}
	nestedRecords := []internalapi.InternalRecord{
		{Did: "did-nested", FileName: ptr("nested/file3.txt")},
	}

	lister := &fakeIndexLister{
		responses: map[string]internalapi.ListRecordsResponse{
			"Ellrott_Lab|hla2vec||10|":         {Records: &rootRecords, Directories: &rootDirs},
			"Ellrott_Lab|hla2vec|data|9|":      {Records: &dataRecords, Directories: &dataDirs},
			"Ellrott_Lab|hla2vec|nested|8|":    {Records: &nestedRecords},
			"Ellrott_Lab|hla2vec|data/more|7|": {Records: &moreRecords},
		},
	}

	records, err := listRecords(context.Background(), lister, syfonclient.ListRecordsOptions{
		Organization: "Ellrott_Lab",
		ProjectID:    "hla2vec",
		Limit:        10,
	}, true)
	if err != nil {
		t.Fatalf("listRecords returned error: %v", err)
	}

	got := make([]string, 0, len(records))
	for _, rec := range records {
		got = append(got, strings.TrimSpace(rec.Did))
	}
	want := []string{"did-root", "did-data", "did-nested", "did-more"}
	if !slices.Equal(got, want) {
		t.Fatalf("unexpected recursive record set: got %v want %v", got, want)
	}
}

func TestListRecordsRecursiveRequiresScopedProject(t *testing.T) {
	_, err := listRecords(context.Background(), &fakeIndexLister{}, syfonclient.ListRecordsOptions{Limit: 10}, true)
	if err == nil || !strings.Contains(err.Error(), "--recursive requires both --organization and --project") {
		t.Fatalf("expected recursive scope error, got %v", err)
	}
}

func TestListRecordsRecursiveRejectsStartAndPage(t *testing.T) {
	_, err := listRecords(context.Background(), &fakeIndexLister{}, syfonclient.ListRecordsOptions{
		Organization: "Ellrott_Lab",
		ProjectID:    "hla2vec",
		Start:        "did-1",
		Limit:        10,
	}, true)
	if err == nil || !strings.Contains(err.Error(), "--recursive does not support --start or --page") {
		t.Fatalf("expected recursive pagination error, got %v", err)
	}
}

func ptr(v string) *string { return &v }
