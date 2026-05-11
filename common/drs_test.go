package common

import (
	"testing"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

func TestDrsObjectMatchesScope(t *testing.T) {
	t.Run("controlled access", func(t *testing.T) {
		obj := &drsapi.DrsObject{
			ControlledAccess: &[]string{"/organization/org/project/proj"},
		}
		if !DrsObjectMatchesScope(obj, "org", "proj") {
			t.Fatal("expected controlled-access match")
		}
	})

	t.Run("access method authz ignored", func(t *testing.T) {
		authz := []string{"/programs/org/projects/proj"}
		obj := &drsapi.DrsObject{
			AccessMethods: &[]drsapi.AccessMethod{{
				Type: drsapi.AccessMethodTypeS3,
				Authorizations: &struct {
					BearerAuthIssuers   *[]string                                          `json:"bearer_auth_issuers,omitempty"`
					DrsObjectId         *string                                            `json:"drs_object_id,omitempty"`
					PassportAuthIssuers *[]string                                          `json:"passport_auth_issuers,omitempty"`
					SupportedTypes      *[]drsapi.AccessMethodAuthorizationsSupportedTypes `json:"supported_types,omitempty"`
				}{BearerAuthIssuers: &authz},
			}},
		}
		if DrsObjectMatchesScope(obj, "org", "proj") {
			t.Fatal("access method authorizations should not define scope")
		}
	})
}
