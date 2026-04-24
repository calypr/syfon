package common

import (
	"reflect"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

func TestDrsUUIDAndBuilder(t *testing.T) {
	t.Run("uuid is deterministic", func(t *testing.T) {
		id1 := DrsUUID("syfon", "e2e", "abc123")
		id2 := DrsUUID("syfon", "e2e", "abc123")
		id3 := DrsUUID("syfon", "other", "abc123")

		if id1 == "" || id2 == "" || id3 == "" {
			t.Fatalf("expected non-empty UUIDs: %q %q %q", id1, id2, id3)
		}
		if id1 != id2 {
			t.Fatalf("expected deterministic UUIDs, got %q and %q", id1, id2)
		}
		if id1 == id3 {
			t.Fatalf("expected scope-sensitive UUIDs, got %q and %q", id1, id3)
		}
	})

	t.Run("builder and object construction", func(t *testing.T) {
		if _, err := BuildDrsObjWithPrefix("file.txt", "", 10, "drs-1", "bucket", "syfon", "e2e", "programs/syfon/projects/e2e"); err == nil {
			t.Fatal("expected checksum validation error")
		}

		obj, err := BuildDrsObjWithPrefix("file.txt", "sha-1", 10, "drs-1", "bucket", "syfon", "e2e", "programs/syfon/projects/e2e")
		if err != nil {
			t.Fatalf("BuildDrsObjWithPrefix returned error: %v", err)
		}
		if obj.Id != "drs-1" || obj.SelfUri != "drs://drs-1" || obj.Size != 10 {
			t.Fatalf("unexpected object fields: %+v", obj)
		}
		if obj.AccessMethods == nil || len(*obj.AccessMethods) != 1 {
			t.Fatalf("expected one access method, got %+v", obj.AccessMethods)
		}
		am := (*obj.AccessMethods)[0]
		if am.Type != drsapi.AccessMethodTypeS3 {
			t.Fatalf("unexpected access method type: %v", am.Type)
		}
		if am.AccessUrl == nil || am.AccessUrl.Url != "s3://bucket/programs/syfon/projects/e2e/sha-1" {
			t.Fatalf("unexpected access url: %+v", am.AccessUrl)
		}
		if am.Authorizations == nil || !reflect.DeepEqual(*am.Authorizations, map[string][]string{"syfon": []string{"e2e"}}) {
			t.Fatalf("unexpected authz map: %+v", am.Authorizations)
		}

		builder := NewObjectBuilder("bucket", "e2e")
		builder.Organization = "syfon"
		built, err := builder.Build("file.txt", "sha-2", 20, "drs-2")
		if err != nil {
			t.Fatalf("builder.Build returned error: %v", err)
		}
		if built.AccessMethods == nil || len(*built.AccessMethods) != 1 {
			t.Fatalf("expected builder access methods, got %+v", built.AccessMethods)
		}
		if got := (*built.AccessMethods)[0].Authorizations; got == nil || !reflect.DeepEqual(*got, map[string][]string{"syfon": []string{"e2e"}}) {
			t.Fatalf("expected org/project authz on built object, got %+v", got)
		}
	})

	t.Run("convert to candidate", func(t *testing.T) {
		obj := &drsapi.DrsObject{
			Id:      "drs-1",
			Name:    strPtr("file.txt"),
			Size:    7,
			Version: strPtr("1"),
			Checksums: []drsapi.Checksum{{
				Type: "sha256", Checksum: "abc",
			}},
		}
		candidate := ConvertToCandidate(obj)
		if candidate.Name == nil || *candidate.Name != "file.txt" {
			t.Fatalf("unexpected candidate: %+v", candidate)
		}
		if candidate.Size != 7 || len(candidate.Checksums) != 1 {
			t.Fatalf("unexpected candidate payload: %+v", candidate)
		}
	})
}

func strPtr(s string) *string { return &s }
