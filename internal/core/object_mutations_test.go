package core

import (
	"context"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db"
)

func ptr[T any](v T) *T { return &v }

func TestRegisterBulk_RegistersCandidate(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)

	candidates := []drs.DrsObjectCandidate{
		{
			Aliases: ptr([]string{"id:test-register-bulk"}),
			Checksums: []drs.Checksum{{
				Type:     "sha256",
				Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			}},
			Size: 1,
		},
	}

	count, err := om.RegisterBulk(context.Background(), candidates)
	if err != nil {
		t.Fatalf("RegisterBulk error: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count=1, got=%d", count)
	}

	obj, err := database.GetObject(context.Background(), "test-register-bulk")
	if err != nil {
		t.Fatalf("expected registered object, got error: %v", err)
	}
	if obj == nil || obj.Id != "test-register-bulk" {
		t.Fatalf("unexpected object: %+v", obj)
	}
}

func TestRegisterBulk_InvalidChecksum(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)

	candidates := []drs.DrsObjectCandidate{{
		Aliases: ptr([]string{"id:test-invalid-checksum"}),
		Checksums: []drs.Checksum{{
			Type:     "md5",
			Checksum: "abc",
		}},
		Size: 1,
	}}

	if _, err := om.RegisterBulk(context.Background(), candidates); err == nil {
		t.Fatalf("expected RegisterBulk error for invalid checksum")
	}
}

func TestBulkDeleteObjects_DeletesAuthorizedObjects(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)

	_, err := om.RegisterBulk(context.Background(), []drs.DrsObjectCandidate{{
		Aliases: ptr([]string{"id:test-delete-bulk"}),
		Checksums: []drs.Checksum{{
			Type:     "sha256",
			Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		}},
		Size: 1,
	}})
	if err != nil {
		t.Fatalf("seed RegisterBulk error: %v", err)
	}

	if err := om.BulkDeleteObjects(context.Background(), []string{"test-delete-bulk"}); err != nil {
		t.Fatalf("BulkDeleteObjects error: %v", err)
	}
	if _, err := database.GetObject(context.Background(), "test-delete-bulk"); err == nil {
		t.Fatalf("expected object to be deleted")
	}
}

