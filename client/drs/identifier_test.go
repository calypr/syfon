package drs

import (
	"testing"

	"github.com/google/uuid"
)

func TestParseObjectIdentifier_UUID(t *testing.T) {
	id := ParseObjectIdentifier("ef979f4a-efcb-516c-b7b4-d4cc9ba6c033")
	if id.Kind() != IdentifierUUID {
		t.Fatalf("expected uuid kind, got %s", id.Kind())
	}
	if id.UUID == uuid.Nil || id.Hash != nil {
		t.Fatalf("unexpected identifier parse result: %+v", id)
	}
}

func TestParseObjectIdentifier_SHA256(t *testing.T) {
	raw := "sha256:40b8b6a2855c3d194df090a2240adf5eb6c49d9b75accfae0d4c69dacb5a2c5d"
	id := ParseObjectIdentifier(raw)
	if id.Kind() != IdentifierChecksum {
		t.Fatalf("expected checksum kind, got %s", id.Kind())
	}
	if id.Hash == nil || id.Hash.Type != "sha256" || id.Hash.Checksum != "40b8b6a2855c3d194df090a2240adf5eb6c49d9b75accfae0d4c69dacb5a2c5d" {
		t.Fatalf("unexpected sha parse result: %+v", id)
	}
}

func TestParseObjectIdentifier_SHA512(t *testing.T) {
	raw := "sha512:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	id := ParseObjectIdentifier(raw)
	if id.Kind() != IdentifierChecksum {
		t.Fatalf("expected checksum kind, got %s", id.Kind())
	}
	if id.Hash == nil || id.Hash.Type != "sha512" || id.Hash.Checksum == "" {
		t.Fatalf("unexpected checksum parse result: %+v", id)
	}
}

func TestParseObjectIdentifier_Unknown(t *testing.T) {
	id := ParseObjectIdentifier("not-an-id")
	if id.Kind() != IdentifierUnknown {
		t.Fatalf("expected unknown kind, got %s", id.Kind())
	}
}
