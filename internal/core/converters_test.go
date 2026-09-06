package core

import (
	"testing"

	"github.com/calypr/syfon/internal/objects"
)

func TestConverters(t *testing.T) {
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
