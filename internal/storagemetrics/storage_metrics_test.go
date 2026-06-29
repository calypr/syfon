package storagemetrics

import (
	"testing"
	"time"

	"github.com/calypr/syfon/internal/models"
)

func TestAggregateStoragePathSummary(t *testing.T) {
	now := time.Now().UTC()
	rows := []models.DrsObjectRecord{
		{Name: "data/a.txt", Size: 10, UpdatedTime: now.Add(-time.Hour), DownloadCount: 2, LastDownloadTime: timePtr(now.Add(-3 * time.Hour))},
		{Path: "data/a.txt", Size: 20, UpdatedTime: now, DownloadCount: 4, LastDownloadTime: timePtr(now.Add(-30 * time.Minute))},
		{Name: "data/nested/b.txt", Size: 30, UpdatedTime: now.Add(-2 * time.Hour), DownloadCount: 1, LastDownloadTime: timePtr(now.Add(-90 * time.Minute))},
		{Name: "other/c.txt", Size: 40, UpdatedTime: now.Add(-3 * time.Hour)},
		{Name: "bad/../path", Size: 50, UpdatedTime: now},
	}

	summary, err := AggregateStoragePathSummary("org", "proj", "data", rows)
	if err != nil {
		t.Fatalf("AggregateStoragePathSummary returned error: %v", err)
	}
	if summary.Path != "data" {
		t.Fatalf("expected normalized path data, got %q", summary.Path)
	}
	if summary.FileCount != 2 || summary.RecordCount != 3 || summary.TotalBytes != 60 {
		t.Fatalf("unexpected counts: %+v", summary)
	}
	if summary.DirectChildCount != 2 {
		t.Fatalf("expected 2 direct children, got %+v", summary)
	}
	if summary.DuplicatePathCount != 1 {
		t.Fatalf("expected one duplicate path, got %+v", summary)
	}
	if summary.DownloadCount != 7 {
		t.Fatalf("unexpected download count: %+v", summary)
	}
	if summary.LastDownloadTime == nil || !summary.LastDownloadTime.Equal(now.Add(-30*time.Minute)) {
		t.Fatalf("unexpected last download time: %+v", summary.LastDownloadTime)
	}
	if summary.LatestUpdateTime == nil || !summary.LatestUpdateTime.Equal(now) {
		t.Fatalf("unexpected latest update time: %+v", summary.LatestUpdateTime)
	}
}

func TestAggregateStoragePathChildren(t *testing.T) {
	now := time.Now().UTC()
	rows := []models.DrsObjectRecord{
		{Name: ` data\\alpha.txt `, Size: 10, UpdatedTime: now.Add(-time.Hour), DownloadCount: 2, LastDownloadTime: timePtr(now.Add(-10 * time.Hour))},
		{Name: "data/dir/one.txt", Size: 15, UpdatedTime: now.Add(-2 * time.Hour), DownloadCount: 3, LastDownloadTime: timePtr(now.Add(-4 * time.Hour))},
		{Name: "data/dir/two.txt", Size: 25, UpdatedTime: now, DownloadCount: 5, LastDownloadTime: timePtr(now.Add(-15 * time.Minute))},
		{Name: "data/alpha.txt", Size: 5, UpdatedTime: now.Add(-30 * time.Minute), DownloadCount: 7, LastDownloadTime: timePtr(now.Add(-2 * time.Hour))},
		{Name: "skip/elsewhere.txt", Size: 99, UpdatedTime: now},
	}

	items, err := AggregateStoragePathChildren("data", rows, 10, 0, "bytes", "desc")
	if err != nil {
		t.Fatalf("AggregateStoragePathChildren returned error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %+v", items)
	}
	if items[0].Name != "dir" || items[0].Type != StoragePathChildTypeDirectory || items[0].FileCount != 2 || items[0].RecordCount != 2 || items[0].TotalBytes != 40 || items[0].DownloadCount != 8 {
		t.Fatalf("unexpected directory aggregate: %+v", items[0])
	}
	if items[0].LastDownloadTime == nil || !items[0].LastDownloadTime.Equal(now.Add(-15*time.Minute)) {
		t.Fatalf("unexpected directory last download time: %+v", items[0].LastDownloadTime)
	}
	if items[1].Name != "alpha.txt" || items[1].Type != StoragePathChildTypeFile || items[1].FileCount != 1 || items[1].RecordCount != 2 || items[1].TotalBytes != 15 || items[1].DownloadCount != 9 {
		t.Fatalf("unexpected file aggregate: %+v", items[1])
	}
	if items[1].LastDownloadTime == nil || !items[1].LastDownloadTime.Equal(now.Add(-2*time.Hour)) {
		t.Fatalf("unexpected file last download time: %+v", items[1].LastDownloadTime)
	}
}

func TestNormalizeStorageChildrenSort(t *testing.T) {
	sortBy, sortOrder, err := NormalizeStorageChildrenSort("", "")
	if err != nil {
		t.Fatalf("NormalizeStorageChildrenSort returned error: %v", err)
	}
	if sortBy != "name" || sortOrder != "asc" {
		t.Fatalf("unexpected defaults: %q %q", sortBy, sortOrder)
	}
	if _, _, err := NormalizeStorageChildrenSort("nope", "asc"); err == nil {
		t.Fatal("expected invalid sort_by error")
	}
	if _, _, err := NormalizeStorageChildrenSort("name", "down"); err == nil {
		t.Fatal("expected invalid sort_order error")
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}
