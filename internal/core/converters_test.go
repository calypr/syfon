package core

import (
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/lfsapi"
)

func TestConverters(t *testing.T) {
	t.Run("candidate to internal object", func(t *testing.T) {
		authz := []string{"/programs/syfon/projects/e2e"}
		url := "https://storage.example/object.bin"
		name := "object.bin"
		size := int64(42)
		candidate := drs.DrsObjectCandidate{
			Name:      &name,
			Size:      size,
			Checksums: []drs.Checksum{{Type: "sha256", Checksum: strings.Repeat("a", 64)}},
			AccessMethods: &[]drs.AccessMethod{{
				Type: "https",
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: url},
			}},
			ControlledAccess: &authz,
		}

		obj, err := CandidateToInternalObject(candidate, time.Unix(123, 0))
		if err != nil {
			t.Fatalf("CandidateToInternalObject returned error: %v", err)
		}
		if projects := obj.Authorizations["syfon"]; len(projects) != 1 || projects[0] != "e2e" {
			t.Fatalf("unexpected internal authz list: %+v", obj.Authorizations)
		}
	})

	t.Run("lfs candidate to drs", func(t *testing.T) {
		url := "https://storage.example/object.bin"
		size := int64(42)
		lfsID := "lfs-explicit-id"
		candidate := lfsapi.DrsObjectCandidate{
			Id:   strPtr(lfsID),
			Name: strPtr("object.bin"),
			Size: &size,
			Checksums: &[]lfsapi.Checksum{{
				Type: "sha256", Checksum: "abc123",
			}},
			AccessMethods: &[]lfsapi.AccessMethod{{
				Type: strPtr("https"),
				AccessUrl: &lfsapi.AccessMethodAccessUrl{
					Url: &url,
				},
			}},
		}

		got := LFSCandidateToDRS(candidate)
		if got.AccessMethods == nil || len(*got.AccessMethods) != 1 {
			t.Fatalf("expected one access method, got %+v", got.AccessMethods)
		}
		if (*got.AccessMethods)[0].Authorizations != nil {
			t.Fatalf("did not expect authz map on access method, got %+v", (*got.AccessMethods)[0].Authorizations)
		}
		if got.Aliases == nil || len(*got.Aliases) == 0 || (*got.Aliases)[0] != "id:"+lfsID {
			t.Fatalf("expected explicit lfs id alias, got %+v", got.Aliases)
		}
	})

	t.Run("lfs candidate to drs defaults id alias to sha256 oid", func(t *testing.T) {
		size := int64(42)
		oid := strings.Repeat("b", 64)
		candidate := lfsapi.DrsObjectCandidate{
			Name: strPtr("object.bin"),
			Size: &size,
			Checksums: &[]lfsapi.Checksum{{
				Type: "sha256", Checksum: oid,
			}},
		}

		got := LFSCandidateToDRS(candidate)
		if got.Aliases == nil || len(*got.Aliases) == 0 || (*got.Aliases)[0] != "id:"+oid {
			t.Fatalf("expected oid-derived id alias, got %+v", got.Aliases)
		}
	})
}

func strPtr(s string) *string { return &s }
