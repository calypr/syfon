package repair

import (
	"context"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
)

func TestStorageCleanupAuditStaleDuplicateRecord(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-live", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-missing", NormalizedPath: "data/file.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-live": cleanupObjectWithChecksum(
				"obj-live",
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"s3://bucket/project/path/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			),
			"obj-missing": cleanupObjectWithChecksum(
				"obj-missing",
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"s3://bucket/11111111-1111-1111-1111-111111111111/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			),
		},
		inspectByURL: map[string]error{
			"s3://bucket/project/path/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":                         nil,
			"s3://bucket/11111111-1111-1111-1111-111111111111/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
		ExpectedPaths: []string{
			"data/file.tsv",
		},
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 1 {
		t.Fatalf("expected one finding, got %d", len(report.Findings))
	}
	if report.Findings[0].Kind != FindingStaleDuplicateRecord {
		t.Fatalf("expected stale duplicate finding, got %s", report.Findings[0].Kind)
	}
	if report.Findings[0].StructuralReason != "same_checksum_same_size_legacy_url_mismatch" {
		t.Fatalf("unexpected structural reason: %q", report.Findings[0].StructuralReason)
	}
	if !report.Findings[0].LegacyURLTemplateDetected {
		t.Fatal("expected legacy url template detection")
	}
}

func TestStorageCleanupAuditLiveDuplicateConflict(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-2", NormalizedPath: "data/file.tsv", Size: 11},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1", "s3://bucket/object-1/file.tsv"),
			"obj-2": cleanupObject("obj-2", "s3://bucket/object-2/file.tsv"),
		},
	}
	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{Organization: "org", Project: "proj"})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 1 || report.Findings[0].Kind != FindingLiveDuplicateConflict {
		t.Fatalf("expected live duplicate conflict, got %+v", report.Findings)
	}
}

func TestStorageCleanupAuditDuplicateWithoutAccessMethodsEmitsProbeError(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-2", NormalizedPath: "data/file.tsv", Size: 11},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1"),
			"obj-2": cleanupObject("obj-2"),
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if report.Scanned != 2 || report.ScannedPaths != 1 || report.ClassifiedPaths != 1 || report.UnclassifiedPaths != 0 {
		t.Fatalf("unexpected counters: %+v", report)
	}
	if len(report.Findings) != 1 || report.Findings[0].Kind != FindingStorageProbeError {
		t.Fatalf("expected one storage probe error, got %+v", report.Findings)
	}
	for _, record := range report.Findings[0].Records {
		if record.StorageStatus != StorageProbeStatusUnknown {
			t.Fatalf("expected unknown status, got %s", record.StorageStatus)
		}
		if record.StorageMessage != "record has no access URLs" {
			t.Fatalf("unexpected storage message: %q", record.StorageMessage)
		}
	}
}

func TestStorageCleanupAuditDuplicateWithOneLiveAndOneUnknownEmitsProbeError(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-live", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-unknown", NormalizedPath: "data/file.tsv", Size: 11},
		},
		objects: map[string]models.InternalObject{
			"obj-live":    cleanupObject("obj-live", "s3://bucket/live.tsv"),
			"obj-unknown": cleanupObject("obj-unknown"),
		},
		inspectByURL: map[string]error{
			"s3://bucket/live.tsv": nil,
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 1 || report.Findings[0].Kind != FindingStorageProbeError {
		t.Fatalf("expected one storage probe error, got %+v", report.Findings)
	}
}

func TestStorageCleanupAuditDuplicateWithEmptyAccessURLsEmitsProbeError(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-2", NormalizedPath: "data/file.tsv", Size: 11},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1", ""),
			"obj-2": cleanupObject("obj-2", " "),
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 1 || report.Findings[0].Kind != FindingStorageProbeError {
		t.Fatalf("expected one storage probe error, got %+v", report.Findings)
	}
	for _, record := range report.Findings[0].Records {
		if record.StorageStatus != StorageProbeStatusUnknown {
			t.Fatalf("expected unknown status, got %s", record.StorageStatus)
		}
		if record.StorageMessage != "record has no access URLs" {
			t.Fatalf("unexpected storage message: %q", record.StorageMessage)
		}
	}
}

func TestStorageCleanupAuditBrokenAccessURLDuplicateDoesNotAbort(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-live", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-broken", NormalizedPath: "data/file.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-live": cleanupObjectWithChecksum(
				"obj-live",
				"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"s3://bucket/project/path/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			),
			"obj-broken": cleanupObjectWithChecksum(
				"obj-broken",
				"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				"s3://old-bucket/22222222-2222-2222-2222-222222222222/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			),
		},
		inspectByURL: map[string]error{
			"s3://bucket/project/path/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": nil,
			"s3://old-bucket/22222222-2222-2222-2222-222222222222/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"old-bucket\"",
			},
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization:  "org",
		Project:       "proj",
		ExpectedPaths: []string{"data/file.tsv"},
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 2 {
		t.Fatalf("expected two findings, got %+v", report.Findings)
	}
	if report.Findings[0].Kind != FindingStorageProbeError || report.Findings[1].Kind != FindingBrokenAccessURLError {
		t.Fatalf("unexpected finding kinds: %+v", report.Findings)
	}
	finding := report.Findings[1]
	if finding.Kind != FindingBrokenAccessURLError {
		t.Fatalf("expected broken access url finding, got %s", finding.Kind)
	}
	if len(finding.Records) != 1 || finding.Records[0].ObjectID != "obj-broken" {
		t.Fatalf("expected only broken record in finding, got %+v", finding.Records)
	}
	if finding.CleanupScope != CleanupScopeRecord {
		t.Fatalf("expected record cleanup scope, got %s", finding.CleanupScope)
	}
	if finding.StructuralReason != "same_checksum_same_size_legacy_url_mismatch" {
		t.Fatalf("unexpected structural reason: %q", finding.StructuralReason)
	}
}

func TestStorageCleanupAuditMixedMissingAndBrokenDuplicateEmitsBothFindings(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-live", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-missing", NormalizedPath: "data/file.tsv", Size: 10},
			{ObjectID: "obj-broken", NormalizedPath: "data/file.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-live":    cleanupObject("obj-live", "s3://bucket/live/file.tsv"),
			"obj-missing": cleanupObject("obj-missing", "s3://bucket/missing/file.tsv"),
			"obj-broken":  cleanupObject("obj-broken", "s3://old-bucket/file.tsv"),
		},
		inspectByURL: map[string]error{
			"s3://bucket/live/file.tsv":    nil,
			"s3://bucket/missing/file.tsv": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
			"s3://old-bucket/file.tsv": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"old-bucket\"",
			},
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization:  "org",
		Project:       "proj",
		ExpectedPaths: []string{"data/file.tsv"},
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 2 {
		t.Fatalf("expected two findings, got %+v", report.Findings)
	}
	if report.Findings[0].Kind != FindingStaleDuplicateRecord || report.Findings[1].Kind != FindingBrokenAccessURLError {
		t.Fatalf("unexpected finding kinds: %+v", report.Findings)
	}
}

func TestStorageCleanupAuditRepoOrphanStatuses(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-live", NormalizedPath: "data/live.tsv", Size: 10},
			{ObjectID: "obj-stale", NormalizedPath: "data/stale.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-live":  cleanupObject("obj-live", "s3://bucket/data/live.tsv"),
			"obj-stale": cleanupObject("obj-stale", "s3://bucket/data/stale.tsv"),
		},
		inspectByURL: map[string]error{
			"s3://bucket/data/live.tsv":  nil,
			"s3://bucket/data/stale.tsv": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
		},
	}
	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization:  "org",
		Project:       "proj",
		ExpectedPaths: []string{"data/expected.tsv"},
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 2 {
		t.Fatalf("expected two orphan findings, got %d", len(report.Findings))
	}
	if report.Findings[0].Kind != FindingRepoOrphanLiveObject || report.Findings[1].Kind != FindingRepoOrphanStaleRecord {
		t.Fatalf("unexpected findings: %+v", report.Findings)
	}
}

func TestStorageCleanupAuditPartialBrokenAccessURLIsAuditOnly(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-partial", NormalizedPath: "data/file.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-partial": cleanupObject("obj-partial",
				"s3://broken-bucket/file.tsv",
				"s3://bucket/live/file.tsv",
			),
		},
		inspectByURL: map[string]error{
			"s3://broken-bucket/file.tsv": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"broken-bucket\"",
			},
			"s3://bucket/live/file.tsv": nil,
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization:  "org",
		Project:       "proj",
		ExpectedPaths: []string{"data/file.tsv"},
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) != 1 {
		t.Fatalf("expected one finding, got %+v", report.Findings)
	}
	finding := report.Findings[0]
	if finding.Kind != FindingBrokenAccessURLError {
		t.Fatalf("expected broken access url finding, got %s", finding.Kind)
	}
	if finding.CleanupScope != CleanupScopeAccessURL {
		t.Fatalf("expected access_url scope, got %s", finding.CleanupScope)
	}
	record := finding.Records[0]
	if record.StorageStatus != StorageProbeStatusLive {
		t.Fatalf("expected live derived status, got %s", record.StorageStatus)
	}
	if record.CleanupScope != CleanupScopeAccessURL {
		t.Fatalf("expected record access_url scope, got %s", record.CleanupScope)
	}
}

func TestStorageCleanupApplyDeletesExpectedRecords(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "dup-live", NormalizedPath: "data/dup.tsv", Size: 10},
			{ObjectID: "dup-missing", NormalizedPath: "data/dup.tsv", Size: 10},
			{ObjectID: "orphan-live", NormalizedPath: "data/orphan.tsv", Size: 10},
			{ObjectID: "orphan-stale", NormalizedPath: "data/stale.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"dup-live":     cleanupObject("dup-live", "s3://bucket/dup/live.tsv"),
			"dup-missing":  cleanupObject("dup-missing", "s3://bucket/dup/missing.tsv"),
			"orphan-live":  cleanupObject("orphan-live", "s3://bucket/data/orphan.tsv"),
			"orphan-stale": cleanupObject("orphan-stale", "s3://bucket/data/stale.tsv"),
		},
		inspectByURL: map[string]error{
			"s3://bucket/dup/live.tsv":    nil,
			"s3://bucket/dup/missing.tsv": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
			"s3://bucket/data/orphan.tsv": nil,
			"s3://bucket/data/stale.tsv":  &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
		},
	}
	svc := NewStorageCleanupService(manager)
	result, err := svc.Apply(context.Background(), StorageCleanupApplyRequest{
		Organization:          "org",
		Project:               "proj",
		ExpectedPaths:         []string{"data/dup.tsv", "data/keep.tsv"},
		DeleteStaleDuplicates: true,
		DeleteRepoOrphans:     true,
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(result.DeletedRecordIDs) != 3 {
		t.Fatalf("expected three deleted records, got %d", len(result.DeletedRecordIDs))
	}
	if got := manager.deleteOpts["dup-missing"]; got.DeleteStorageData {
		t.Fatal("stale duplicate delete should not purge storage")
	}
	if got := manager.deleteOpts["orphan-live"]; !got.DeleteStorageData {
		t.Fatal("live orphan delete should purge storage")
	}
	if got := manager.deleteOpts["orphan-stale"]; got.DeleteStorageData {
		t.Fatal("stale orphan delete should not purge storage")
	}
}

func TestStorageCleanupApplySkipsPartialBrokenAccessRecord(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-partial", NormalizedPath: "data/file.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-partial": cleanupObject("obj-partial",
				"s3://broken-bucket/file.tsv",
				"s3://bucket/live/file.tsv",
			),
		},
		inspectByURL: map[string]error{
			"s3://broken-bucket/file.tsv": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"broken-bucket\"",
			},
			"s3://bucket/live/file.tsv": nil,
		},
	}

	svc := NewStorageCleanupService(manager)
	result, err := svc.Apply(context.Background(), StorageCleanupApplyRequest{
		Organization:          "org",
		Project:               "proj",
		ExpectedPaths:         []string{"data/file.tsv"},
		DeleteStaleDuplicates: true,
		DeleteRepoOrphans:     true,
	})
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	if len(result.DeletedRecordIDs) != 0 {
		t.Fatalf("expected no deleted records, got %+v", result.DeletedRecordIDs)
	}
}

func TestStorageCleanupAuditUsesBulkObjectFetchOnce(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "obj-1", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "obj-2", NormalizedPath: "data/b.tsv", Size: 11},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1", "s3://bucket/a.tsv"),
			"obj-2": cleanupObject("obj-2", "s3://bucket/b.tsv"),
		},
	}

	svc := NewStorageCleanupService(manager)
	if _, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	}); err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if manager.bulkCalls != 1 {
		t.Fatalf("expected one bulk object fetch, got %d", manager.bulkCalls)
	}
}

func TestStorageCleanupAuditUsesDuplicateListingWithoutExpectedPaths(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "dup-1", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "dup-2", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "single", NormalizedPath: "data/b.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"dup-1":  cleanupObject("dup-1", "s3://bucket/data/a.tsv"),
			"dup-2":  cleanupObject("dup-2", "s3://bucket/data/a-copy.tsv"),
			"single": cleanupObject("single", "s3://bucket/data/b.tsv"),
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if report.Scanned != 2 {
		t.Fatalf("expected duplicate-only scan count 2, got %d", report.Scanned)
	}
	if report.ScannedPaths != 1 {
		t.Fatalf("expected one scanned path, got %d", report.ScannedPaths)
	}
	if manager.duplicateListCalls != 1 || manager.listCalls != 0 {
		t.Fatalf("expected duplicate listing only, got list=%d duplicate=%d", manager.listCalls, manager.duplicateListCalls)
	}
}

func TestStorageCleanupAuditUsesFullListingForOrphanAudit(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/a.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1", "s3://bucket/data/a.tsv"),
		},
	}

	svc := NewStorageCleanupService(manager)
	if _, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization:  "org",
		Project:       "proj",
		ExpectedPaths: []string{"data/other.tsv"},
	}); err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if manager.listCalls != 1 || manager.duplicateListCalls != 0 {
		t.Fatalf("expected full listing only, got list=%d duplicate=%d", manager.listCalls, manager.duplicateListCalls)
	}
}

func TestStorageCleanupAuditCachesBrokenBucketFailures(t *testing.T) {
	manager := &fakeCleanupManager{
		rows: []models.StorageCleanupRecord{
			{ObjectID: "obj-1", NormalizedPath: "data/a.tsv", Size: 10},
			{ObjectID: "obj-2", NormalizedPath: "data/a.tsv", Size: 10},
		},
		objects: map[string]models.InternalObject{
			"obj-1": cleanupObject("obj-1", "s3://old-bucket/data/a.tsv"),
			"obj-2": cleanupObject("obj-2", "s3://old-bucket/data/b.tsv"),
		},
		inspectByURL: map[string]error{
			"s3://old-bucket/data/a.tsv": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"old-bucket\"",
			},
			"s3://old-bucket/data/b.tsv": &core.StorageInspectError{
				Kind:    core.StorageInspectCredentialMissing,
				Message: "no stored bucket credential found for bucket \"old-bucket\"",
			},
		},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
	})
	if err != nil {
		t.Fatalf("Audit failed: %v", err)
	}
	if len(report.Findings) == 0 {
		t.Fatalf("expected findings for broken duplicate path")
	}
	if manager.inspectCalls != 1 {
		t.Fatalf("expected one inspect call after broken bucket cache, got %d", manager.inspectCalls)
	}
}

type fakeCleanupManager struct {
	rows               []models.StorageCleanupRecord
	objects            map[string]models.InternalObject
	inspectByURL       map[string]error
	deleteOpts         map[string]core.DeleteOptions
	bulkCalls          int
	listCalls          int
	duplicateListCalls int
	inspectCalls       int
}

func (f *fakeCleanupManager) ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error) {
	f.listCalls++
	return append([]models.StorageCleanupRecord(nil), filterCleanupRowsByPrefix(f.rows, pathPrefix)...), nil
}

func (f *fakeCleanupManager) ListDuplicateStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error) {
	f.duplicateListCalls++
	rows := filterCleanupRowsByPrefix(f.rows, pathPrefix)
	counts := make(map[string]int, len(rows))
	for _, row := range rows {
		counts[row.NormalizedPath]++
	}
	out := make([]models.StorageCleanupRecord, 0, len(rows))
	for _, row := range rows {
		if counts[row.NormalizedPath] > 1 {
			out = append(out, row)
		}
	}
	return out, nil
}

func (f *fakeCleanupManager) GetBulkObjects(ctx context.Context, ids []string, requiredMethod string) ([]models.InternalObject, error) {
	f.bulkCalls++
	out := make([]models.InternalObject, 0, len(ids))
	for _, id := range ids {
		if obj, ok := f.objects[id]; ok {
			out = append(out, obj)
		}
	}
	return out, nil
}

func (f *fakeCleanupManager) InspectStorageObject(ctx context.Context, req core.InspectStorageRequest) (*core.StorageObjectMetadata, error) {
	f.inspectCalls++
	if err, ok := f.inspectByURL[req.ObjectURL]; ok {
		if err != nil {
			return nil, err
		}
	}
	return &core.StorageObjectMetadata{ObjectURL: req.ObjectURL}, nil
}

func (f *fakeCleanupManager) DeleteObjectWithOptions(ctx context.Context, id string, opts core.DeleteOptions) error {
	if f.deleteOpts == nil {
		f.deleteOpts = map[string]core.DeleteOptions{}
	}
	f.deleteOpts[id] = opts
	return nil
}

func cleanupObject(id string, urls ...string) models.InternalObject {
	methods := make([]drsapi.AccessMethod, 0, len(urls))
	for _, url := range urls {
		methods = append(methods, drsapi.AccessMethod{
			Type: drsapi.AccessMethodTypeS3,
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: url},
		})
	}
	return models.InternalObject{
		DrsObject: drsapi.DrsObject{
			Id:            id,
			AccessMethods: &methods,
		},
	}
}

func cleanupObjectWithChecksum(id string, checksum string, urls ...string) models.InternalObject {
	obj := cleanupObject(id, urls...)
	obj.Checksums = []drsapi.Checksum{{
		Type:     "sha256",
		Checksum: checksum,
	}}
	return obj
}

func filterCleanupRowsByPrefix(rows []models.StorageCleanupRecord, pathPrefix string) []models.StorageCleanupRecord {
	normalizedPrefix, prefixSegments, err := common.NormalizeBrowsePath(pathPrefix)
	if err != nil {
		return nil
	}
	if normalizedPrefix == "" {
		return append([]models.StorageCleanupRecord(nil), rows...)
	}
	out := make([]models.StorageCleanupRecord, 0, len(rows))
	for _, row := range rows {
		info, ok, err := common.BrowsePathInfoFromName(row.NormalizedPath)
		if err != nil || !ok || !common.HasBrowsePathPrefix(info.Segments, prefixSegments) {
			continue
		}
		out = append(out, row)
	}
	return out
}
