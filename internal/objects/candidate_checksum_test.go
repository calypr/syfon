package objects

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
)

func TestMaterializeCandidateRejectsConflictingValidSHA256Values(t *testing.T) {
	candidate := drs.DrsObjectCandidate{
		Aliases: &[]string{"id:conflicting"},
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: strings.Repeat("a", 64)},
			{Type: "SHA-256", Checksum: strings.Repeat("b", 64)},
		},
		AccessMethods: &[]drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://bucket/object"}}},
	}

	if _, err := MaterializeCandidate(candidate, time.Unix(0, 0)); !errors.Is(err, errorapi.ErrConflictingSHA256) {
		t.Fatalf("MaterializeCandidate() error = %v, want ErrConflictingSHA256", err)
	}
}

func TestMaterializeCandidateKeepsEquivalentNormalizedSHA256Values(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	candidate := drs.DrsObjectCandidate{
		Aliases: &[]string{"id:equivalent"},
		Checksums: []drs.Checksum{
			{Type: "SHA-256", Checksum: " SHA256:" + strings.ToUpper(checksum) + " "},
			{Type: "sha256", Checksum: checksum},
		},
		AccessMethods: &[]drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://bucket/object"}}},
	}

	got, err := MaterializeCandidate(candidate, time.Unix(0, 0))
	if err != nil {
		t.Fatalf("MaterializeCandidate() error = %v", err)
	}
	if len(got.Checksums) != 1 || got.Checksums[0] != (drs.Checksum{Type: "sha256", Checksum: checksum}) {
		t.Fatalf("MaterializeCandidate() checksums = %#v, want one normalized SHA-256 %q", got.Checksums, checksum)
	}
}
