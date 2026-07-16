package core

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
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
			AccessMethods: &[]drs.AccessMethod{{
				Type: "s3",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/test-register-bulk"},
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
		AccessMethods: &[]drs.AccessMethod{{
			Type: "s3",
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/test-delete-bulk"},
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

func TestRegisterObjects_CanonicalizesProjectChecksumDuplicates(t *testing.T) {
	database := db.NewInMemoryDB()
	om := NewObjectManager(database, nil)
	now := time.Now().UTC()
	later := now.Add(time.Minute)
	accessURL1 := "s3://bucket/original"
	accessURL2 := "s3://bucket/renamed"

	first := models.InternalObject{
		Authorizations: map[string][]string{"org": {"proj"}},
		DrsObject: drs.DrsObject{
			Id:          "did-1",
			Name:        ptr("original.tsv"),
			Size:        42,
			CreatedTime: now,
			UpdatedTime: &now,
			Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}},
			AccessMethods: &[]drs.AccessMethod{{
				Type: "s3",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: accessURL1},
			}},
		},
	}
	second := models.InternalObject{
		Authorizations: map[string][]string{"org": {"proj"}},
		DrsObject: drs.DrsObject{
			Id:          "did-2",
			Name:        ptr("renamed.tsv"),
			Size:        42,
			CreatedTime: later,
			UpdatedTime: &later,
			Checksums:   first.Checksums,
			AccessMethods: &[]drs.AccessMethod{{
				Type: "s3",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: accessURL2},
			}},
		},
	}

	if err := om.RegisterObjects(context.Background(), []models.InternalObject{first}); err != nil {
		t.Fatalf("RegisterObjects(first) error: %v", err)
	}
	if err := om.RegisterObjects(context.Background(), []models.InternalObject{second}); err != nil {
		t.Fatalf("RegisterObjects(second) error: %v", err)
	}

	records, err := om.GetObjectsByChecksum(context.Background(), first.Checksums[0].Checksum, "")
	if err != nil {
		t.Fatalf("GetObjectsByChecksum error: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 canonical record, got %d", len(records))
	}
	if records[0].Id != "did-1" {
		t.Fatalf("expected canonical did-1, got %q", records[0].Id)
	}
	if got := records[0].Name; got == nil || *got != "renamed.tsv" {
		t.Fatalf("expected latest name renamed.tsv, got %+v", got)
	}
	if !slices.Equal(records[0].NameAliases, []string{"original.tsv"}) {
		t.Fatalf("unexpected name aliases: %#v", records[0].NameAliases)
	}

	aliasObj, err := om.GetObject(context.Background(), "did-2", "")
	if err != nil {
		t.Fatalf("GetObject(alias) error: %v", err)
	}
	if aliasObj.Id != "did-2" {
		t.Fatalf("expected alias lookup to preserve requested did, got %q", aliasObj.Id)
	}
	scopeIDs, err := om.ListObjectIDsByScope(context.Background(), "org", "proj", "")
	if err != nil {
		t.Fatalf("ListObjectIDsByScope error: %v", err)
	}
	if !slices.Equal(scopeIDs, []string{"did-1"}) {
		t.Fatalf("unexpected scoped ids: %#v", scopeIDs)
	}
}
