package core

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/db/sqlite"
	"github.com/calypr/syfon/internal/models"
)

func registerScopedCandidate(t *testing.T, om *ObjectManager, id, checksum, org, project string) {
	t.Helper()
	controlled := []string{"/organization/" + org + "/project/" + project}
	_, err := om.RegisterBulk(context.Background(), []drs.DrsObjectCandidate{{
		Aliases:          ptr([]string{"id:" + id}),
		ControlledAccess: &controlled,
		Checksums: []drs.Checksum{{
			Type:     "sha256",
			Checksum: checksum,
		}},
		AccessMethods: &[]drs.AccessMethod{{
			Type: "s3",
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/" + id},
		}},
		Size: 1,
	}})
	if err != nil {
		t.Fatalf("RegisterBulk(%s): %v", id, err)
	}
}

func TestListObjectIDsPageByChecksum_StartAfterAndLimit(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)
	checksum := "1111111111111111111111111111111111111111111111111111111111111111"

	registerScopedCandidate(t, om, "chk-a", checksum, "org1", "proj1")
	registerScopedCandidate(t, om, "chk-b", checksum, "org1", "proj1")
	registerScopedCandidate(t, om, "chk-c", checksum, "org1", "proj1")

	ids, err := om.ListObjectIDsPageByChecksum(context.Background(), checksum, "sha256", "", "", "read", "chk-a", 2, 0)
	if err != nil {
		t.Fatalf("ListObjectIDsPageByChecksum error: %v", err)
	}
	if len(ids) != 2 || ids[0] != "chk-b" || ids[1] != "chk-c" {
		t.Fatalf("unexpected page ids: %+v", ids)
	}
}

func TestListObjectIDsPageByScope_StartAfterAndScopeFilter(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)
	checksumA := "2222222222222222222222222222222222222222222222222222222222222222"
	checksumB := "3333333333333333333333333333333333333333333333333333333333333333"

	registerScopedCandidate(t, om, "scope-a", checksumA, "org1", "proj1")
	registerScopedCandidate(t, om, "scope-b", checksumB, "org1", "proj1")
	registerScopedCandidate(t, om, "scope-c", "4444444444444444444444444444444444444444444444444444444444444444", "org1", "proj2")

	ids, err := om.ListObjectIDsPageByScope(context.Background(), "org1", "proj1", "read", "scope-a", 10, 0)
	if err != nil {
		t.Fatalf("ListObjectIDsPageByScope error: %v", err)
	}
	if len(ids) != 1 || ids[0] != "scope-b" {
		t.Fatalf("unexpected scoped page ids: %+v", ids)
	}
}

func TestListObjectIDsByScope_AuthzFiltering(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)
	checksum := "5555555555555555555555555555555555555555555555555555555555555555"

	registerScopedCandidate(t, om, "secure-obj", checksum, "secure", "p1")

	unauthorizedCtx := buildLocalAuthzContext(map[string]map[string]bool{
		"/organization/other/project/p2": {"read": true},
	})
	ids, err := om.ListObjectIDsByScope(unauthorizedCtx, "secure", "p1", "read")
	if err != nil {
		t.Fatalf("ListObjectIDsByScope unauthorized error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected no ids for unauthorized context, got %+v", ids)
	}

	authorizedCtx := buildLocalAuthzContext(map[string]map[string]bool{
		"/organization/secure/project/p1": {"read": true},
	})
	ids, err = om.ListObjectIDsByScope(authorizedCtx, "secure", "p1", "read")
	if err != nil {
		t.Fatalf("ListObjectIDsByScope authorized error: %v", err)
	}
	if len(ids) != 1 || ids[0] != "secure-obj" {
		t.Fatalf("expected secure-obj, got %+v", ids)
	}
}

func TestSearchAfterID(t *testing.T) {
	ids := []string{"a", "b", "c", "d"}
	if got := searchAfterID(ids, "b"); got != 2 {
		t.Fatalf("expected index 2 for startAfter=b, got %d", got)
	}
	if got := searchAfterID(ids, "bb"); got != 2 {
		t.Fatalf("expected index 2 for startAfter=bb, got %d", got)
	}
	if got := searchAfterID(ids, "z"); got != len(ids) {
		t.Fatalf("expected index %d for startAfter=z, got %d", len(ids), got)
	}
}

func TestObjectMatchesScope(t *testing.T) {
	obj := &models.InternalObject{Authorizations: map[string][]string{"org1": {"p1", "p2"}}}
	if !objectMatchesScope(obj, "org1", "p1") {
		t.Fatalf("expected org1/p1 to match")
	}
	if objectMatchesScope(obj, "org1", "p3") {
		t.Fatalf("expected org1/p3 not to match")
	}
	if !objectMatchesScope(obj, "org1", "") {
		t.Fatalf("expected org-wide org1 to match")
	}
}

type trackingDB struct {
	db.DatabaseInterface
	bulkCalls [][]string
}

func (t *trackingDB) GetBulkObjects(ctx context.Context, ids []string) ([]models.InternalObject, error) {
	copyIDs := append([]string(nil), ids...)
	t.bulkCalls = append(t.bulkCalls, copyIDs)
	return t.DatabaseInterface.GetBulkObjects(ctx, ids)
}

func TestPrepareScopedObjects_HydratesOnlyMissingSiblingIDs(t *testing.T) {
	base, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("NewSqliteDB failed: %v", err)
	}
	tracked := &trackingDB{DatabaseInterface: base}
	om := NewObjectManager(tracked, nil)
	checksum := "6666666666666666666666666666666666666666666666666666666666666666"
	controlled := []string{"/organization/org/project/proj"}

	for _, obj := range []models.InternalObject{
		{
			Authorizations: map[string][]string{"org": {"proj"}},
			DrsObject: drs.DrsObject{
				Id:               "dup-a",
				CreatedTime:      drsISOTime("2026-01-01T00:00:00Z"),
				UpdatedTime:      ptrTime("2026-01-01T00:00:00Z"),
				Checksums:        []drs.Checksum{{Type: "sha256", Checksum: checksum}},
				ControlledAccess: &controlled,
				AccessMethods: &[]drs.AccessMethod{{
					Type: drs.AccessMethodTypeS3,
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://bucket/dup-a"},
				}},
			},
		},
		{
			Authorizations: map[string][]string{"org": {"proj"}},
			DrsObject: drs.DrsObject{
				Id:               "dup-b",
				CreatedTime:      drsISOTime("2026-01-02T00:00:00Z"),
				UpdatedTime:      ptrTime("2026-01-02T00:00:00Z"),
				Checksums:        []drs.Checksum{{Type: "sha256", Checksum: checksum}},
				ControlledAccess: &controlled,
				AccessMethods: &[]drs.AccessMethod{{
					Type: drs.AccessMethodTypeS3,
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: "s3://bucket/dup-b"},
				}},
			},
		},
	} {
		if err := tracked.CreateObject(context.Background(), &obj); err != nil {
			t.Fatalf("CreateObject failed: %v", err)
		}
	}

	initial, err := tracked.GetBulkObjects(context.Background(), []string{"dup-a"})
	if err != nil {
		t.Fatalf("GetBulkObjects failed: %v", err)
	}
	tracked.bulkCalls = nil

	prepared, err := om.PrepareScopedObjects(context.Background(), initial, "org", "proj", "")
	if err != nil {
		t.Fatalf("PrepareScopedObjects failed: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("expected 1 canonical record, got %d", len(prepared))
	}
	if prepared[0].AccessMethods == nil || len(*prepared[0].AccessMethods) != 2 {
		t.Fatalf("expected merged access methods, got %+v", prepared[0].AccessMethods)
	}
	if len(tracked.bulkCalls) != 1 {
		t.Fatalf("expected 1 sibling hydration call, got %d", len(tracked.bulkCalls))
	}
	if !slices.Equal(tracked.bulkCalls[0], []string{"dup-b"}) {
		t.Fatalf("expected only missing sibling id to be hydrated, got %+v", tracked.bulkCalls[0])
	}
}

func drsISOTime(raw string) time.Time {
	tm, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		panic(err)
	}
	return tm
}

func ptrTime(raw string) *time.Time {
	tm := drsISOTime(raw)
	return &tm
}

func TestReadableChecksumFilter(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)

	unenforcedCtx := context.Background()
	res, includeUnscoped, restrict, ok := om.readableChecksumFilter(unenforcedCtx, "", "")
	if !ok || includeUnscoped || restrict || res != nil {
		t.Fatalf("unexpected unenforced filter: res=%+v includeUnscoped=%v restrict=%v ok=%v", res, includeUnscoped, restrict, ok)
	}

	forbiddenCtx := buildGen3Context(map[string]map[string]bool{})
	res, includeUnscoped, restrict, ok = om.readableChecksumFilter(forbiddenCtx, "", "")
	if !ok || !includeUnscoped || !restrict {
		t.Fatalf("expected restricted filter under enforced authz, got res=%+v includeUnscoped=%v restrict=%v ok=%v", res, includeUnscoped, restrict, ok)
	}
}
