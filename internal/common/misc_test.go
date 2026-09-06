package common

import (
	"encoding/json"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/models"
)

func TestInternalObjectExternal(t *testing.T) {
	obj := models.InternalObject{DrsObject: drs.DrsObject{Id: "obj-1", Name: Ptr("n")}}
	ext := obj.External()
	if ext.Id != "obj-1" || StringVal(ext.Name) != "n" {
		t.Fatalf("unexpected external object: %+v", ext)
	}
}

func TestInternalObjectJSONAliases(t *testing.T) {
	raw := []byte(`{"did":"obj-1","size":7,"controlled_access":["https://calypr.org/program/test/project/proj"],"authorizations":{"legacy":[]},"hashes":{"sha256":"abc"},"extra":"keep-me"}`)

	var obj models.InternalObject
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if obj.Id != "obj-1" {
		t.Fatalf("expected did to map to id, got %q", obj.Id)
	}
	if obj.Size != 7 {
		t.Fatalf("expected size 7, got %d", obj.Size)
	}
	if got := obj.Authorizations["test"]; len(got) != 1 || got[0] != "proj" {
		t.Fatalf("expected controlled_access to derive authorizations, got %v", obj.Authorizations)
	}
	if len(obj.Checksums) != 1 || obj.Checksums[0].Type != "sha256" || obj.Checksums[0].Checksum != "abc" {
		t.Fatalf("expected hashes to map to checksums, got %+v", obj.Checksums)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var roundTripped map[string]any
	if err := json.Unmarshal(out, &roundTripped); err != nil {
		t.Fatalf("decode marshaled payload: %v", err)
	}
	if got := roundTripped["did"]; got != "obj-1" {
		t.Fatalf("expected did alias in output, got %v", got)
	}
	if got := roundTripped["id"]; got != "obj-1" {
		t.Fatalf("expected id in output, got %v", got)
	}
	if got := roundTripped["extra"]; got != "keep-me" {
		t.Fatalf("expected unknown fields to survive, got %v", got)
	}
	if _, ok := roundTripped["authorizations"]; ok {
		t.Fatalf("expected retired authorizations field to be omitted, got %v", roundTripped)
	}
	if _, ok := roundTripped["auth"]; ok {
		t.Fatalf("expected retired auth field to be omitted, got %v", roundTripped)
	}
	if _, ok := roundTripped["controlled_access"]; !ok {
		t.Fatalf("expected controlled_access field in output, got %v", roundTripped)
	}
}
