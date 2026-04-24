package common

import (
	"strings"
	"testing"
)

func TestMintObjectIDFromChecksum(t *testing.T) {
	valid := strings.Repeat("a", 64)
	id1 := MintObjectIDFromChecksum(valid, []string{"/programs/syfon/projects/e2e"})
	id2 := MintObjectIDFromChecksum(valid, []string{"/programs/syfon/projects/e2e"})
	id3 := MintObjectIDFromChecksum(valid, []string{"/programs/syfon/projects/other"})
	if id1 == "" || id2 == "" || id3 == "" {
		t.Fatalf("expected non-empty object ids: %q %q %q", id1, id2, id3)
	}
	if id1 != id2 {
		t.Fatalf("expected deterministic object ids, got %q and %q", id1, id2)
	}
	if id1 == id3 {
		t.Fatalf("expected authz-sensitive object ids, got %q and %q", id1, id3)
	}
}
