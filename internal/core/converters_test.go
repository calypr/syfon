package core

import (
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/lfsapi"
)

func TestUniqueAuthzAndConverters(t *testing.T) {
	t.Run("unique authz flattens map", func(t *testing.T) {
		authz := map[string][]string{
			"syfon": []string{"e2e", "e2e-2"},
			"other": []string{},
		}
		got := UniqueAuthz([]drs.AccessMethod{{
			Authorizations: &authz,
		}})
		want := []string{"/programs/syfon/projects/e2e", "/programs/syfon/projects/e2e-2", "/programs/other"}
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("unexpected authz list length: got=%v want=%v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("unexpected authz list: got=%v want=%v", got, want)
			}
		}
	})

	t.Run("candidate to internal object", func(t *testing.T) {
		authz := map[string][]string{"syfon": []string{"e2e"}}
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
				Authorizations: &authz,
			}},
		}

		obj, err := CandidateToInternalObject(candidate, time.Unix(123, 0))
		if err != nil {
			t.Fatalf("CandidateToInternalObject returned error: %v", err)
		}
		if len(obj.Authorizations) != 1 || obj.Authorizations[0] != "/programs/syfon/projects/e2e" {
			t.Fatalf("unexpected internal authz list: %+v", obj.Authorizations)
		}
	})

	t.Run("lfs candidate to drs", func(t *testing.T) {
		url := "https://storage.example/object.bin"
		size := int64(42)
		authz := []string{"/programs/syfon/projects/e2e"}
		candidate := lfsapi.DrsObjectCandidate{
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
				Authorizations: &lfsapi.AccessMethodAuthorizations{
					BearerAuthIssuers: &authz,
				},
			}},
		}

		got := LFSCandidateToDRS(candidate)
		if got.AccessMethods == nil || len(*got.AccessMethods) != 1 {
			t.Fatalf("expected one access method, got %+v", got.AccessMethods)
		}
		if (*got.AccessMethods)[0].Authorizations == nil || len(*(*got.AccessMethods)[0].Authorizations) != 1 {
			t.Fatalf("expected authz map on access method, got %+v", (*got.AccessMethods)[0].Authorizations)
		}
	})
}

func strPtr(s string) *string { return &s }
