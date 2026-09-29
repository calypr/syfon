package objects_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/objects"
)

func TestGetObjectsMatchesGetObjectForEachIdentifier(t *testing.T) {
	database := newSQLiteDatabase(t)
	const allowed = "/organization/org/project/allowed"
	const denied = "/organization/org/project/denied"
	sharedSHA := strings.Repeat("a", 64)
	collisionSHA := strings.Repeat("c", 64)
	otherSHA := strings.Repeat("d", 64)
	genericSHA := strings.Repeat("e", 64)
	method := func(key string) *[]drs.AccessMethod {
		return &[]drs.AccessMethod{{Type: "s3", AccessUrl: &drs.AccessURL{Url: "s3://bucket/" + key}}}
	}
	rows := []drs.DrsObject{
		{Id: "shared-private", Checksums: []drs.Checksum{{Type: "sha256", Checksum: sharedSHA}}, ControlledAccess: &[]string{denied}, AccessMethods: method("private")},
		{Id: "shared-public", Checksums: []drs.Checksum{{Type: "sha256", Checksum: sharedSHA}}, AccessMethods: method("public")},
		{Id: "allowed", ControlledAccess: &[]string{allowed}, AccessMethods: method("allowed")},
		{Id: "orphan", ControlledAccess: &[]string{denied}, AccessMethods: method("orphan")},
		{Id: collisionSHA, Checksums: []drs.Checksum{{Type: "sha256", Checksum: otherSHA}}, AccessMethods: method("collision")},
		{Id: "checksum-target", Checksums: []drs.Checksum{{Type: "sha256", Checksum: collisionSHA}}, AccessMethods: method("target")},
		{Id: "generic-first", Checksums: []drs.Checksum{{Type: "md5", Checksum: "generic-md5"}, {Type: "sha256", Checksum: genericSHA}}, ControlledAccess: &[]string{allowed}, AccessMethods: method("generic-first")},
		{Id: "generic-sibling", Checksums: []drs.Checksum{{Type: "sha256", Checksum: genericSHA}}, ControlledAccess: &[]string{denied}, AccessMethods: method("generic-sibling")},
	}
	if err := database.RegisterObjects(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if err := database.CreateObjectAlias(context.Background(), "allowed-alias", "allowed"); err != nil {
		t.Fatal(err)
	}
	service := objects.NewService(database)
	identifiers := []string{"shared-private", "shared-public", sharedSHA, "SHA256:" + strings.ToUpper(sharedSHA), "allowed", " allowed-alias ", "orphan", "missing", collisionSHA, "generic-md5", "generic-sibling", "", "allowed"}
	for _, scenario := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "authorized", ctx: buildLocalAuthzContext(map[string]map[string]bool{allowed: {"read": true}})},
		{name: "no scoped access", ctx: buildLocalAuthzContext(nil)},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			got, err := service.GetObjects(scenario.ctx, identifiers, "read")
			if err != nil {
				t.Fatal(err)
			}
			for _, raw := range identifiers {
				identifier := strings.TrimSpace(raw)
				wantObject, wantErr := service.GetObject(scenario.ctx, identifier, "read")
				actual, ok := got[identifier]
				if !ok {
					t.Errorf("%q missing from result", identifier)
					continue
				}
				if !sameLookupError(actual.Err, wantErr) {
					t.Errorf("%q error = %v, want %v", identifier, actual.Err, wantErr)
				}
				if !reflect.DeepEqual(actual.Object, wantObject) {
					t.Errorf("%q object = %+v, want %+v", identifier, actual.Object, wantObject)
				}
			}
			if got[collisionSHA].Object == nil || got[collisionSHA].Object.Id != "checksum-target" {
				t.Errorf("SHA identity did not win physical ID collision: %+v", got[collisionSHA])
			}
			if got["orphan"].Object != nil || !errors.Is(got["orphan"].Err, errorapi.ErrAccessDenied) {
				t.Errorf("private orphan = %+v", got["orphan"])
			}
			if got["missing"].Object != nil || !errors.Is(got["missing"].Err, errorapi.ErrObjectNotFound) {
				t.Errorf("missing = %+v", got["missing"])
			}
		})
	}
}

func sameLookupError(actual, want error) bool {
	if actual == nil || want == nil {
		return actual == nil && want == nil
	}
	return errors.Is(actual, want) || errors.Is(want, actual)
}
