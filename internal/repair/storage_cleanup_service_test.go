package repair

import (
	"context"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/server/drs"
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
			"obj-live":    cleanupObject("obj-live", "s3://bucket/live/file.tsv"),
			"obj-missing": cleanupObject("obj-missing", "s3://bucket/missing/file.tsv"),
		},
	}
	manager.inspectByObject = map[string]error{
		"obj-live":    nil,
		"obj-missing": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
	}

	svc := NewStorageCleanupService(manager)
	report, err := svc.Audit(context.Background(), StorageCleanupAuditRequest{
		Organization: "org",
		Project:      "proj",
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
		inspectByObject: map[string]error{
			"obj-live":  nil,
			"obj-stale": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
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
		inspectByObject: map[string]error{
			"dup-live":     nil,
			"dup-missing":  &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
			"orphan-live":  nil,
			"orphan-stale": &core.StorageInspectError{Kind: core.StorageInspectObjectNotFound, Message: "missing"},
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

type fakeCleanupManager struct {
	rows            []models.StorageCleanupRecord
	objects         map[string]models.InternalObject
	inspectByObject map[string]error
	deleteOpts      map[string]core.DeleteOptions
}

func (f *fakeCleanupManager) ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error) {
	return append([]models.StorageCleanupRecord(nil), f.rows...), nil
}

func (f *fakeCleanupManager) GetBulkObjects(ctx context.Context, ids []string, requiredMethod string) ([]models.InternalObject, error) {
	out := make([]models.InternalObject, 0, len(ids))
	for _, id := range ids {
		if obj, ok := f.objects[id]; ok {
			out = append(out, obj)
		}
	}
	return out, nil
}

func (f *fakeCleanupManager) InspectStorageObject(ctx context.Context, req core.InspectStorageRequest) (*core.StorageObjectMetadata, error) {
	for id, obj := range f.objects {
		for _, method := range derefServerMethods(obj.AccessMethods) {
			if method.AccessUrl != nil && method.AccessUrl.Url == req.ObjectURL {
				if err, ok := f.inspectByObject[id]; ok {
					return nil, err
				}
				return &core.StorageObjectMetadata{ObjectURL: req.ObjectURL}, nil
			}
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

func cleanupObject(id, url string) models.InternalObject {
	methods := []drsapi.AccessMethod{{
		Type: drsapi.AccessMethodTypeS3,
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: url},
	}}
	return models.InternalObject{
		DrsObject: drsapi.DrsObject{
			Id:            id,
			AccessMethods: &methods,
		},
	}
}

func derefServerMethods(methods *[]drsapi.AccessMethod) []drsapi.AccessMethod {
	if methods == nil {
		return nil
	}
	return *methods
}
