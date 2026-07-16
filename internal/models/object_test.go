package models

import (
	"encoding/json"
	"testing"
)

func TestInternalObjectRejectsLegacyPathAliases(t *testing.T) {
	raw := []byte(`{
		"did":"did-1",
		"file_name":"legacy-name.txt",
		"hashes":{"sha256":"abc"},
		"urls":["legacy"],
		"unknown_field":"keep-me"
	}`)
	var obj InternalObject
	if err := json.Unmarshal(raw, &obj); err == nil {
		t.Fatal("expected file_name payload to be rejected")
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
