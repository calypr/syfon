package core

import (
	"context"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db"
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

