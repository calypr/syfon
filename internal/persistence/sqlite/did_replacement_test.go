package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
)

func TestReplaceObjectAtomicallyReplacesPhysicalDIDMetadataAndPolicy(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	newScope := "/organization/org/project/new"
	oldSHA := strings.Repeat("a", 64)
	newSHA := strings.Repeat("b", 64)
	old := identityTestObject("physical-did", oldSHA, "", "s3://bucket/old")
	if err := db.RegisterObjects(context.Background(), []drs.DrsObject{old}); err != nil {
		t.Fatalf("seed old object: %v", err)
	}
	replacement := identityTestObject("physical-did", newSHA, newScope, "s3://bucket/new")
	replacement.Size = 0
	replacement.Name = sqliteTestPtr("replacement.bin")
	if err := db.ReplaceObject(context.Background(), replacement.Id, oldSHA, replacement); err != nil {
		t.Fatalf("ReplaceObject() error = %v", err)
	}

	got, err := db.GetObject(ctx, replacement.Id)
	if err != nil {
		t.Fatalf("GetObject after replacement: %v", err)
	}
	if got.Checksums[0].Checksum != newSHA || got.Size != 0 || got.Name == nil || *got.Name != "replacement.bin" {
		t.Fatalf("replacement metadata = %+v, want SHA %s, size 0, and new name", got, newSHA)
	}
	if got.AccessMethods == nil || len(*got.AccessMethods) != 1 || (*got.AccessMethods)[0].AccessUrl.Url != "s3://bucket/new" {
		t.Fatalf("replacement access methods = %+v", got.AccessMethods)
	}
	if got.ControlledAccess == nil || len(*got.ControlledAccess) != 1 || (*got.ControlledAccess)[0] != newScope {
		t.Fatalf("replacement controlled access = %+v, want only %q", got.ControlledAccess, newScope)
	}
	var publicRead bool
	if err := db.DB().QueryRow(`SELECT public_read FROM drs_object_read_policy WHERE object_id = ?`, replacement.Id).Scan(&publicRead); err != nil {
		t.Fatal(err)
	}
	if publicRead {
		t.Fatal("replacement retained public-read policy from the old public object")
	}
}

func TestReplaceObjectRequiresOldDeleteAndNewCreate(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	oldScope := "/organization/org/project/old"
	newScope := "/organization/org/project/new"
	oldSHA := strings.Repeat("a", 64)
	newSHA := strings.Repeat("b", 64)
	old := identityTestObject("auth-did", oldSHA, oldScope, "s3://bucket/old")
	if err := db.RegisterObjects(testIdentityAuth(oldScope, "create", "read", "delete"), []drs.DrsObject{old}); err != nil {
		t.Fatal(err)
	}
	replacement := identityTestObject("auth-did", newSHA, newScope, "s3://bucket/new")

	noDelete := withIdentityPrivileges(context.Background(), newScope, "create")
	if err := db.ReplaceObject(noDelete, replacement.Id, oldSHA, replacement); !errors.Is(err, errorapi.ErrAccessDenied) {
		t.Fatalf("ReplaceObject without old delete = %v, want access denied", err)
	}
	noCreate := withIdentityPrivileges(context.Background(), oldScope, "delete")
	if err := db.ReplaceObject(noCreate, replacement.Id, oldSHA, replacement); !errors.Is(err, errorapi.ErrAccessDenied) {
		t.Fatalf("ReplaceObject without new create = %v, want access denied", err)
	}
	assertDIDReplacementUnchanged(t, db, replacement.Id, oldSHA, "s3://bucket/old")
}

func TestReplaceObjectChecksExpectedOldSHAInsideTransaction(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	scope := "/organization/org/project/p"
	oldSHA := strings.Repeat("a", 64)
	currentSHA := strings.Repeat("c", 64)
	newSHA := strings.Repeat("b", 64)
	old := identityTestObject("precondition-did", currentSHA, scope, "s3://bucket/current")
	if err := db.RegisterObjects(testIdentityAuth(scope, "create", "read", "delete"), []drs.DrsObject{old}); err != nil {
		t.Fatal(err)
	}
	replacement := identityTestObject("precondition-did", newSHA, scope, "s3://bucket/new")
	ctx := testIdentityAuth(scope, "create", "read", "delete")
	if err := db.ReplaceObject(ctx, replacement.Id, oldSHA, replacement); !errors.Is(err, errorapi.ErrConflict) {
		t.Fatalf("ReplaceObject stale precondition = %v, want conflict", err)
	}
	assertDIDReplacementUnchanged(t, db, replacement.Id, currentSHA, "s3://bucket/current")
}

func TestReplaceObjectRollsBackWhenNewChecksumWriteFails(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	scope := "/organization/org/project/p"
	oldSHA := strings.Repeat("a", 64)
	newSHA := strings.Repeat("b", 64)
	old := identityTestObject("rollback-did", oldSHA, scope, "s3://bucket/old")
	if err := db.RegisterObjects(testIdentityAuth(scope, "create", "read", "delete"), []drs.DrsObject{old}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `CREATE TRIGGER fail_did_replace_checksum BEFORE INSERT ON drs_object_checksum WHEN NEW.checksum = '`+newSHA+`' BEGIN SELECT RAISE(ABORT, 'blocked checksum'); END`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.DB().ExecContext(ctx, `DROP TRIGGER fail_did_replace_checksum`) })
	replacement := identityTestObject("rollback-did", newSHA, scope, "s3://bucket/new")
	if err := db.ReplaceObject(testIdentityAuth(scope, "create", "delete"), replacement.Id, oldSHA, replacement); err == nil {
		t.Fatal("ReplaceObject succeeded despite checksum trigger")
	}
	assertDIDReplacementUnchanged(t, db, replacement.Id, oldSHA, "s3://bucket/old")
}

func TestReplaceObjectRebindsAliasWithoutDeletingOldCanonicalRecord(t *testing.T) {
	db, err := NewSqliteDB(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	oldScope := "/organization/org/project/old"
	newScope := "/organization/org/project/new"
	oldSHA := strings.Repeat("a", 64)
	newSHA := strings.Repeat("b", 64)
	admin := testIdentityAuth(oldScope, "create", "read", "delete")
	old := identityTestObject("old-canonical", oldSHA, oldScope, "s3://bucket/old")
	if err := db.RegisterObjects(admin, []drs.DrsObject{old}); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{"replacement-alias", "sibling-alias"} {
		aliasRecord := identityTestObject(alias, oldSHA, oldScope, "s3://bucket/old")
		if err := db.RegisterObjects(admin, []drs.DrsObject{aliasRecord}); err != nil {
			t.Fatalf("seed %s: %v", alias, err)
		}
	}
	replacement := identityTestObject("replacement-alias", newSHA, newScope, "s3://bucket/new")
	writeCtx := testIdentityAuth(oldScope, "delete")
	writeCtx = withIdentityPrivileges(writeCtx, newScope, "create")
	if err := db.ReplaceObject(writeCtx, replacement.Id, oldSHA, replacement); err != nil {
		t.Fatalf("ReplaceObject(alias) error = %v", err)
	}
	newObject, err := db.GetObject(ctx, "replacement-alias")
	if err != nil || newObject.Id != "replacement-alias" || newObject.Checksums[0].Checksum != newSHA {
		t.Fatalf("replacement alias lookup = %+v, %v", newObject, err)
	}
	oldObject, err := db.GetObject(ctx, "old-canonical")
	if err != nil || oldObject.Checksums[0].Checksum != oldSHA {
		t.Fatalf("old canonical lookup = %+v, %v", oldObject, err)
	}
	sibling, err := db.GetObject(ctx, "sibling-alias")
	if err != nil || sibling.Id != "old-canonical" || sibling.Checksums[0].Checksum != oldSHA {
		t.Fatalf("sibling alias lookup = %+v, %v", sibling, err)
	}
}

func assertDIDReplacementUnchanged(t *testing.T, db interface {
	GetObject(context.Context, string) (*drs.DrsObject, error)
}, id, wantSHA, wantURL string) {
	t.Helper()
	got, err := db.GetObject(context.Background(), id)
	if err != nil {
		t.Fatalf("GetObject(%q) after rejected replacement: %v", id, err)
	}
	if got.Checksums[0].Checksum != wantSHA || got.AccessMethods == nil || len(*got.AccessMethods) != 1 || (*got.AccessMethods)[0].AccessUrl.Url != wantURL {
		t.Fatalf("object after rejected replacement = %+v, want SHA %s and URL %q", got, wantSHA, wantURL)
	}
}
