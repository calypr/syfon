package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/google/uuid"
)

func TestPostgresContentIdentityRejectsAliasesThatNamePhysicalObjects(t *testing.T) {
	db := openPostgresUsageTestStore(t)
	ctx := context.Background()
	suffix := uuid.NewString()
	canonicalID := "alias-collision-canonical-" + suffix
	registerAliasID := "alias-collision-register-" + suffix
	replaceAliasID := "alias-collision-replace-" + suffix
	createAliasID := "alias-collision-create-" + suffix
	now := time.Now().UTC()
	canonical := postgresUsageObject(canonicalID, "canonical-"+suffix, now)
	registerAlias := postgresUsageObject(registerAliasID, "register-"+suffix, now)
	replaceAlias := postgresUsageObject(replaceAliasID, "replace-"+suffix, now)
	createAlias := postgresUsageObject(createAliasID, "create-"+suffix, now)
	if err := db.RegisterObjects(ctx, []drs.DrsObject{canonical, registerAlias, replaceAlias, createAlias}); err != nil {
		t.Fatalf("register physical objects: %v", err)
	}

	registration := postgresUsageObject("alias-collision-new-"+suffix, "canonical-"+suffix, now)
	registration.Aliases = &[]string{"id:" + registerAliasID}
	if err := db.RegisterObjects(ctx, []drs.DrsObject{registration}); !errors.Is(err, errorapi.ErrConflict) {
		t.Fatalf("registration alias collision error = %v, want conflict", err)
	}

	replacement, err := db.GetObject(ctx, canonicalID)
	if err != nil {
		t.Fatalf("load canonical object: %v", err)
	}
	attemptedName := "should-not-commit"
	replacement.Name = &attemptedName
	replacement.Aliases = &[]string{"id:" + replaceAliasID}
	if err := db.ReplaceObjects(ctx, []drs.DrsObject{*replacement}); !errors.Is(err, errorapi.ErrConflict) {
		t.Fatalf("replacement alias collision error = %v, want conflict", err)
	}
	if err := db.CreateObjectAlias(ctx, createAliasID, canonicalID); !errors.Is(err, errorapi.ErrConflict) {
		t.Fatalf("CreateObjectAlias physical ID collision error = %v, want conflict", err)
	}

	var aliasRows, physicalRows int
	if err := db.DB().QueryRowContext(ctx, `SELECT count(*) FROM drs_object_alias`).Scan(&aliasRows); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT count(*) FROM drs_object`).Scan(&physicalRows); err != nil {
		t.Fatal(err)
	}
	if aliasRows != 0 || physicalRows != 4 {
		t.Fatalf("after rejected collisions got %d aliases and %d physical rows, want 0 and 4", aliasRows, physicalRows)
	}
	for _, id := range []string{registerAliasID, replaceAliasID, createAliasID} {
		resolved, err := db.ResolveObjectAlias(ctx, id)
		if !errors.Is(err, errorapi.ErrNotFound) {
			t.Fatalf("ResolveObjectAlias(%q) = %q, %v; physical IDs must not be aliases", id, resolved, err)
		}
		got, err := db.GetObject(ctx, id)
		if err != nil || got.Id != id {
			t.Fatalf("physical object %q lookup = %+v, %v", id, got, err)
		}
	}
	if err := db.DeleteObject(ctx, registerAliasID); err != nil {
		t.Fatalf("delete unrelated physical object: %v", err)
	}
	if _, err := db.GetObject(ctx, registerAliasID); !errors.Is(err, errorapi.ErrObjectNotFound) {
		t.Fatalf("deleted physical ID lookup error = %v, want object not found", err)
	}
	if _, err := db.ResolveObjectAlias(ctx, registerAliasID); !errors.Is(err, errorapi.ErrNotFound) {
		t.Fatalf("deleted physical ID alias resolution error = %v, want not found", err)
	}
	unchanged, err := db.GetObject(ctx, canonicalID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Name != nil || (unchanged.AccessMethods != nil && len(*unchanged.AccessMethods) != 0) {
		t.Fatalf("rejected operations changed canonical object: %+v", unchanged)
	}
}
