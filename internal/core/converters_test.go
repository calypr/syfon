package core

import (
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/objects"
)

func TestConverters(t *testing.T) {
	t.Run("candidate to internal object", func(t *testing.T) {
		authz := []string{"/programs/syfon/projects/e2e"}
		url := "https://storage.example/object.bin"
		name := "object.bin"
		size := int64(42)
		candidate := objects.Candidate{
			Name:      &name,
			Size:      &size,
			Checksums: &[]objects.Checksum{{Type: "sha256", Checksum: strings.Repeat("a", 64)}},
			AccessMethods: &[]objects.AccessMethod{{
				Type:      "https",
				AccessUrl: &objects.AccessURL{Url: url},
			}},
			ControlledAccess: &authz,
		}

		obj, err := CandidateToRecord(candidate, time.Unix(123, 0))
		if err != nil {
			t.Fatalf("CandidateToRecord returned error: %v", err)
		}
		if projects := obj.Authorizations["syfon"]; len(projects) != 1 || projects[0] != "e2e" {
			t.Fatalf("unexpected internal authz list: %+v", obj.Authorizations)
		}
	})

	t.Run("candidate without access methods fails", func(t *testing.T) {
		size := int64(42)
		candidate := objects.Candidate{
			Size:      &size,
			Checksums: &[]objects.Checksum{{Type: "sha256", Checksum: strings.Repeat("b", 64)}},
		}
		if _, err := CandidateToRecord(candidate, time.Unix(123, 0)); err == nil || !strings.Contains(err.Error(), "access method") {
			t.Fatalf("expected access-method validation error, got %v", err)
		}
	})

	t.Run("enforce canonical project scope appends exact controlled access", func(t *testing.T) {
		obj, err := EnforceCanonicalProjectScope(objects.Record{

			Id:             "obj-1",
			Authorizations: map[string][]string{"other": {"proj"}},
		}, "org", "proj")
		if err != nil {
			t.Fatalf("EnforceCanonicalProjectScope returned error: %v", err)
		}
		got := ObjectAccessResources(&obj)
		if len(got) != 2 || got[0] != "/organization/other/project/proj" || got[1] != "/organization/org/project/proj" {
			t.Fatalf("unexpected controlled access: %+v", got)
		}
	})
}
