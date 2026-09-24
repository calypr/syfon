package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/sqlite"
)

func TestRepairPreservesWorkingLocationWithoutConfirmedReplacement(t *testing.T) {
	for _, test := range []struct {
		name       string
		probeError error
	}{
		{name: "missing replacement"},
		{name: "provider failure", probeError: errors.New("provider unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db, err := sqlite.NewSqliteDB(":memory:", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			sha := strings.Repeat("a", 64)
			original := "s3://repair-bucket/prefix/legacy/working-object"
			canonical := "s3://repair-bucket/prefix/did-1/" + sha
			records := objects.NewService(db)
			if _, err := records.RegisterObjects(ctx, []drs.DrsObject{repairRecord("did-1", sha, original)}); err != nil {
				t.Fatal(err)
			}
			probe := &fakeRepairProbe{missing: map[string]bool{canonical: true, "s3://repair-bucket/prefix/file.txt": true}, providerError: test.probeError}
			service := newRepairTestService(records, repairBuckets(), probe)
			result, err := service.ApplyAuthorized(ctx, internalapi.ScopeRepairOptions{Organization: "org", Project: "project", CheckStorage: true})
			if err != nil {
				t.Fatal(err)
			}
			got, err := db.GetObject(ctx, "did-1")
			if err != nil {
				t.Fatal(err)
			}
			if got.AccessMethods == nil || len(*got.AccessMethods) != 1 {
				t.Fatalf("stored access methods = %+v", got.AccessMethods)
			}
			if location := (*got.AccessMethods)[0].AccessUrl.Url; location != original {
				t.Errorf("stored URL = %q, want working URL %q", location, original)
			}
			if result.Mutated != 0 || result.AutoFixable != 0 {
				t.Errorf("repair mutated=%d auto_fixable=%d, want 0, 0", result.Mutated, result.AutoFixable)
			}
		})
	}
}

func TestRepairRestoresMissingScopeForDeterministicOrphan(t *testing.T) {
	ctx := context.Background()
	db, err := sqlite.NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	resource := "/organization/org/project/project"
	sha := strings.Repeat("a", 64)
	id, err := objects.MintRecordIDFromChecksum(sha, []string{resource})
	if err != nil {
		t.Fatal(err)
	}
	otherSHA := strings.Repeat("b", 64)
	otherID, err := objects.MintRecordIDFromChecksum(otherSHA, []string{"/organization/other/project/other"})
	if err != nil {
		t.Fatal(err)
	}
	makeOrphan := func(id, checksum string) drs.DrsObject {
		return drs.DrsObject{
			Id:        id,
			Checksums: []drs.Checksum{{Type: "sha256", Checksum: checksum}},
			AccessMethods: &[]drs.AccessMethod{{
				Type:      "s3",
				AccessUrl: &drs.AccessURL{Url: "s3://repair-bucket/prefix/" + id + "/" + checksum},
			}},
		}
	}
	records := objects.NewService(db)
	if _, err := records.RegisterObjects(ctx, []drs.DrsObject{
		makeOrphan(id, sha),
		makeOrphan(otherID, otherSHA),
	}); err != nil {
		t.Fatalf("RegisterObjects: %v", err)
	}
	visible, err := records.ListObjects(ctx, objects.RecordListQuery{
		Scope:          objects.Scope{Organization: "org", Project: "project"},
		RequiredMethod: "read",
		Limit:          10,
	})
	if err != nil || len(visible) != 0 {
		t.Fatalf("ordinary scoped list returned %d records, error %v; want zero before repair", len(visible), err)
	}

	service := newRepairTestService(records, repairBuckets(), nil)
	readCtx := repairAuthzContext(map[string]map[string]bool{resource: {"read": true}})
	audit, err := service.AuditAuthorized(readCtx, internalapi.ScopeRepairOptions{Organization: "org", Project: "project"})
	if err != nil {
		t.Fatalf("AuditAuthorized: %v", err)
	}
	if audit.Scanned < 1 || len(audit.Objects) != 1 || audit.Objects[0].ObjectId != id || !audit.Objects[0].AutoFixable {
		t.Fatalf("audit report = %+v, want one auto-fixable deterministic orphan", audit)
	}

	updateCtx := repairAuthzContext(map[string]map[string]bool{resource: {"read": true, "update": true}})
	result, err := service.ApplyAuthorized(updateCtx, internalapi.ScopeRepairOptions{Organization: "org", Project: "project"})
	if err != nil {
		t.Fatalf("ApplyAuthorized: %v", err)
	}
	if result.Mutated != 1 || len(result.Report.Objects) != 1 {
		t.Fatalf("repair result = %+v, want one deterministic orphan repaired", result)
	}
	if got := result.Report.Objects[0]; got.ObjectId != id || len(got.Findings) != 1 || got.Findings[0].Kind != FindingMissingControlledAccess || !got.AutoFixable {
		t.Fatalf("repair finding = %+v, want one auto-fixable missing controlled access finding for %s", got, id)
	}

	repaired, err := db.GetObject(ctx, id)
	if err != nil {
		t.Fatalf("GetObject repaired record: %v", err)
	}
	if repaired.ControlledAccess == nil || len(*repaired.ControlledAccess) != 1 || (*repaired.ControlledAccess)[0] != resource {
		t.Fatalf("repaired controlled access = %v, want [%s]", repaired.ControlledAccess, resource)
	}
	untouched, err := db.GetObject(ctx, otherID)
	if err != nil {
		t.Fatalf("GetObject unrelated record: %v", err)
	}
	if untouched.ControlledAccess != nil && len(*untouched.ControlledAccess) != 0 {
		t.Fatalf("unrelated controlled access = %v, want unchanged", untouched.ControlledAccess)
	}
}
