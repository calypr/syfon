package common

import (
	"reflect"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

func TestAccessMethodAuthorizationsRoundTrip(t *testing.T) {
	authzMap := map[string][]string{"org": {"proj"}}
	got := AuthzMapFromAccessMethodAuthorizations(AccessMethodAuthorizationsFromAuthzMap(authzMap))
	if !reflect.DeepEqual(got, authzMap) {
		t.Fatalf("unexpected round trip authz map: got=%v want=%v", got, authzMap)
	}
}

func TestEnsureAccessMethodAuthorizations(t *testing.T) {
	t.Run("no access methods", func(t *testing.T) {
		obj := &drsapi.DrsObject{}
		got, changed := EnsureAccessMethodAuthorizations(obj, map[string][]string{"org": {"proj"}})
		if got != obj {
			t.Fatal("expected original object pointer")
		}
		if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("fills missing authz only", func(t *testing.T) {
		obj := &drsapi.DrsObject{
			AccessMethods: &[]drsapi.AccessMethod{{
				Type: drsapi.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/object"},
			}},
		}

		_, changed := EnsureAccessMethodAuthorizations(obj, map[string][]string{"org": {"proj"}})
		if !changed {
			t.Fatal("expected change")
		}
		method := (*obj.AccessMethods)[0]
		if method.AccessUrl == nil || method.AccessUrl.Url != "s3://bucket/object" {
			t.Fatalf("access url changed unexpectedly: %+v", method.AccessUrl)
		}
		got := AuthzMapFromAccessMethodAuthorizations(method.Authorizations)
		want := map[string][]string{"org": {"proj"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected authz map: got=%v want=%v", got, want)
		}
	})

	t.Run("preserves existing authz", func(t *testing.T) {
		orig := AccessMethodAuthorizationsFromAuthzMap(map[string][]string{"keep": {"me"}})
		obj := &drsapi.DrsObject{
			AccessMethods: &[]drsapi.AccessMethod{{
				Type:           drsapi.AccessMethodTypeS3,
				Authorizations: orig,
			}},
		}

		_, changed := EnsureAccessMethodAuthorizations(obj, map[string][]string{"org": {"proj"}})
		if changed {
			t.Fatal("expected no change")
		}
		got := AuthzMapFromAccessMethodAuthorizations((*obj.AccessMethods)[0].Authorizations)
		want := map[string][]string{"keep": {"me"}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("existing authz overwritten: got=%v want=%v", got, want)
		}
	})
}

func TestDrsObjectMatchesScope(t *testing.T) {
	t.Run("controlled access", func(t *testing.T) {
		obj := &drsapi.DrsObject{
			ControlledAccess: &[]string{"/programs/org/projects/proj"},
		}
		if !DrsObjectMatchesScope(obj, "org", "proj") {
			t.Fatal("expected controlled-access match")
		}
	})

	t.Run("access method authz", func(t *testing.T) {
		obj := &drsapi.DrsObject{
			AccessMethods: &[]drsapi.AccessMethod{{
				Type:           drsapi.AccessMethodTypeS3,
				Authorizations: AccessMethodAuthorizationsFromAuthzMap(map[string][]string{"org": {}}),
			}},
		}
		if !DrsObjectMatchesScope(obj, "org", "any-project") {
			t.Fatal("expected org-wide access-method match")
		}
	})
}
