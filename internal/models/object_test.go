package models

import (
	"encoding/json"
	"testing"
)

func TestInternalObjectUnmarshalLegacyAndMarshalCompatibility(t *testing.T) {
	raw := []byte(`{
		"did":"did-1",
		"file_name":"legacy-name.txt",
		"hashes":{"sha256":"abc"},
		"urls":["legacy"],
		"unknown_field":"keep-me"
	}`)
	var obj InternalObject
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if obj.Id != "did-1" {
		t.Fatalf("expected id from did, got %q", obj.Id)
	}
	if obj.Name == nil || *obj.Name != "legacy-name.txt" {
		t.Fatalf("expected name from file_name, got %+v", obj.Name)
	}
	if len(obj.Checksums) != 1 || obj.Checksums[0].Type != "sha256" || obj.Checksums[0].Checksum != "abc" {
		t.Fatalf("unexpected checksums: %+v", obj.Checksums)
	}

	encoded, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(encoded, &out); err != nil {
		t.Fatalf("decode marshaled payload: %v", err)
	}
	if out["did"] != "did-1" {
		t.Fatalf("expected did in output, got %v", out["did"])
	}
	if out["file_name"] != "legacy-name.txt" {
		t.Fatalf("expected file_name in output, got %v", out["file_name"])
	}
	if _, ok := out["unknown_field"]; !ok {
		t.Fatalf("expected unknown field preservation in output")
	}
	if _, retired := out["urls"]; retired {
		t.Fatalf("expected retired auth fields removed from output")
	}
}

func TestInternalObjectExternal(t *testing.T) {
	obj := InternalObject{}
	obj.Id = "did-2"
	external := obj.External()
	if external.Id != "did-2" {
		t.Fatalf("expected external object id did-2, got %q", external.Id)
	}
}

