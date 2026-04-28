package migrate

import (
	"testing"
	"time"
)

func TestTransformIndexdRecord(t *testing.T) {
	size := int64(42)
	name := "file.bam"
	version := "v1"
	desc := "source record"
	created := "2024-03-04T05:06:07Z"
	rec := IndexdRecord{
		DID:         "dg.test/abc",
		Size:        &size,
		FileName:    &name,
		Version:     &version,
		Description: &desc,
		CreatedDate: &created,
		URLs:        []string{"s3://bucket/key", "s3://bucket/key", "gs://bucket/key"},
		Hashes:      map[string]string{"sha256": "abc123", "md5": "def456"},
		Authz:       []string{"/programs/ohsu/projects/brca", "/programs/ohsu/projects/brca"},
	}

	got, err := Transform(rec, nil, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	if err != nil {
		t.Fatalf("Transform returned error: %v", err)
	}
	if got.ID != rec.DID || got.Size != size {
		t.Fatalf("identity fields not preserved: %+v", got)
	}
	if got.Name == nil || *got.Name != name || got.Version == nil || *got.Version != version || got.Description == nil || *got.Description != desc {
		t.Fatalf("metadata not preserved: %+v", got)
	}
	if len(got.Checksums) != 2 {
		t.Fatalf("expected 2 checksums, got %+v", got.Checksums)
	}
	if len(got.AccessMethods) != 2 {
		t.Fatalf("expected de-duplicated access methods, got %+v", got.AccessMethods)
	}
	if got.AccessMethods[0].Authorizations == nil || len((*got.AccessMethods[0].Authorizations)["ohsu"]) != 1 {
		t.Fatalf("expected authz map on access method, got %+v", got.AccessMethods[0].Authorizations)
	}
	if got.CreatedTime.Format(time.RFC3339) != created {
		t.Fatalf("created time not parsed: %s", got.CreatedTime.Format(time.RFC3339))
	}
}

func TestTransformAppliesDefaultAuthz(t *testing.T) {
	size := int64(1)
	rec := IndexdRecord{
		DID:    "dg.test/default-authz",
		Size:   &size,
		URLs:   []string{"s3://bucket/key"},
		Hashes: map[string]string{"sha256": "abc123"},
	}

	got, err := Transform(rec, []string{"/programs/open"}, time.Now())
	if err != nil {
		t.Fatalf("Transform returned error: %v", err)
	}
	if len(got.Authz) != 1 || got.Authz[0] != "/programs/open" {
		t.Fatalf("expected default authz, got %+v", got.Authz)
	}
	internal, err := MigrationRecordToInternalObject(got)
	if err != nil {
		t.Fatalf("MigrationRecordToInternalObject returned error: %v", err)
	}
	if internal.Authorizations == nil || len(internal.Authorizations["open"]) != 0 {
		t.Fatalf("unexpected internal authorizations: %+v", internal.Authorizations)
	}
}
