package repair

import (
	"context"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/models"
)

func TestProjectDiffAuditWithManifest(t *testing.T) {
	lastDownload := time.Date(2026, 6, 25, 12, 0, 0, 0, time.UTC)
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "dup-1", NormalizedPath: "data/a.tsv", Size: 10, DownloadCount: 2, LastDownloadTime: &lastDownload},
			{ObjectID: "dup-2", NormalizedPath: "data/a.tsv", Size: 10, DownloadCount: 3},
			{ObjectID: "syfon-only", NormalizedPath: "data/b.tsv", Size: 7, DownloadCount: 1},
		},
	}

	report, err := NewProjectDiffService(manager).Audit(context.Background(), ProjectDiffAuditRequest{
		Organization:  "org",
		Project:       "proj",
		PathPrefix:    "data",
		ExpectedPaths: []string{"data/a.tsv", "data/c.tsv"},
	})
	if err != nil {
		t.Fatalf("Audit returned error: %v", err)
	}

	if manager.listCalls != 1 {
		t.Fatalf("expected full subtree listing once, got %d", manager.listCalls)
	}
	if manager.duplicateListCalls != 0 {
		t.Fatalf("expected duplicate-only listing to be skipped, got %d", manager.duplicateListCalls)
	}
	if got := report.Summary.TotalFindings; got != 3 {
		t.Fatalf("expected 3 findings, got %d", got)
	}
	if got := report.Summary.CountsByKind[FindingDuplicateSyfonPaths]; got != 1 {
		t.Fatalf("expected 1 duplicate finding, got %d", got)
	}
	if got := report.Summary.CountsByKind[FindingSyfonMissingInRepo]; got != 1 {
		t.Fatalf("expected 1 syfon-only finding, got %d", got)
	}
	if got := report.Summary.CountsByKind[FindingRepoMissingInSyfon]; got != 1 {
		t.Fatalf("expected 1 repo-only finding, got %d", got)
	}
	if got := report.Summary.MatchedPathCount; got != 1 {
		t.Fatalf("expected 1 matched path, got %d", got)
	}
	if got := report.Summary.IndexedPathCount; got != 2 {
		t.Fatalf("expected 2 indexed paths, got %d", got)
	}
	if got := report.Summary.ScannedRecordCount; got != 3 {
		t.Fatalf("expected 3 scanned records, got %d", got)
	}
}

func TestProjectDiffAuditWithoutManifestUsesDuplicateListing(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "dup-1", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "dup-2", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "single", NormalizedPath: "data/b.tsv", Size: 7},
		},
	}

	report, err := NewProjectDiffService(manager).Audit(context.Background(), ProjectDiffAuditRequest{
		Organization: "org",
		Project:      "proj",
		PathPrefix:   "data",
	})
	if err != nil {
		t.Fatalf("Audit returned error: %v", err)
	}

	if manager.listCalls != 0 {
		t.Fatalf("expected full subtree listing to be skipped, got %d", manager.listCalls)
	}
	if manager.duplicateListCalls != 1 {
		t.Fatalf("expected duplicate-only listing once, got %d", manager.duplicateListCalls)
	}
	if got := report.Summary.TotalFindings; got != 1 {
		t.Fatalf("expected 1 finding, got %d", got)
	}
	if got := report.Summary.IndexedPathCount; got != 1 {
		t.Fatalf("expected 1 indexed path in duplicate mode, got %d", got)
	}
}
