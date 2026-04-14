package core

import (
	"testing"

	"github.com/google/uuid"
)

func TestMintObjectIDFromChecksum_UsesOnlyFirstAuthzEntry(t *testing.T) {
	checksum := "71bc015b78426c5271cdadddac8e6764f5399e280f3eb5aae86cc4530f5a4359"

	id1 := MintObjectIDFromChecksum(checksum, []string{
		"/programs/calypr/projects/end_to_end_test",
		"/programs/other/projects/ignored",
	})
	id2 := MintObjectIDFromChecksum(checksum, []string{
		"/programs/calypr/projects/end_to_end_test",
		"/programs/another/projects/also_ignored",
	})
	if id1 != id2 {
		t.Fatalf("expected IDs to match when first authz entry is the same: %s vs %s", id1, id2)
	}
	if _, err := uuid.Parse(id1); err != nil {
		t.Fatalf("expected UUID, got %q", id1)
	}
}

func TestMintObjectIDFromChecksum_FirstAuthzOrderMatters(t *testing.T) {
	checksum := "71bc015b78426c5271cdadddac8e6764f5399e280f3eb5aae86cc4530f5a4359"

	idA := MintObjectIDFromChecksum(checksum, []string{
		"/programs/calypr/projects/end_to_end_test",
		"/programs/other/projects/p2",
	})
	idB := MintObjectIDFromChecksum(checksum, []string{
		"/programs/other/projects/p2",
		"/programs/calypr/projects/end_to_end_test",
	})
	if idA == idB {
		t.Fatalf("expected IDs to differ when first authz entry differs, got same ID %s", idA)
	}
}

func TestMintObjectIDFromChecksum_NormalizesChecksumPrefixAndCase(t *testing.T) {
	authz := []string{"/programs/calypr/projects/end_to_end_test"}
	plain := "71bc015b78426c5271cdadddac8e6764f5399e280f3eb5aae86cc4530f5a4359"
	prefixedUpper := "sha256:71BC015B78426C5271CDADDDAC8E6764F5399E280F3EB5AAE86CC4530F5A4359"

	id1 := MintObjectIDFromChecksum(plain, authz)
	id2 := MintObjectIDFromChecksum(prefixedUpper, authz)
	if id1 != id2 {
		t.Fatalf("expected normalized checksum forms to mint same ID: %s vs %s", id1, id2)
	}
}

func TestMintObjectIDFromChecksum_NoScopeFallbackDeterministic(t *testing.T) {
	checksum := "71bc015b78426c5271cdadddac8e6764f5399e280f3eb5aae86cc4530f5a4359"

	id1 := MintObjectIDFromChecksum(checksum, nil)
	id2 := MintObjectIDFromChecksum(checksum, []string{"not-a-scope"})
	if id1 != id2 {
		t.Fatalf("expected deterministic no-scope fallback IDs to match: %s vs %s", id1, id2)
	}
}
