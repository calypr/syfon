package core

import (
	"context"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
)

func TestBulkOverwriteObjects_ReplacesProjectChecksumSibling(t *testing.T) {
	resource, err := sycommon.ResourcePath("org", "project")
	if err != nil {
		t.Fatal(err)
	}
	sha := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	oldName := "old"
	newName := "new"
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Objects: map[string]*drs.DrsObject{
			"target-did": {Id: "target-did", Name: &oldName, Checksums: []drs.Checksum{{Type: "sha256", Checksum: sha}}},
		},
		ObjectAuthz: map[string]map[string][]string{"target-did": {"org": {"project"}}},
	}}
	om := NewObjectManager(db, &capturingURLManager{})
	candidate := models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:               "source-did",
			Name:             &newName,
			Checksums:        []drs.Checksum{{Type: "sha256", Checksum: sha}},
			ControlledAccess: &[]string{resource},
		},
		Authorizations: map[string][]string{"org": {"project"}},
	}
	result, err := om.BulkOverwriteObjects(buildGen3Context(map[string]map[string]bool{resource: {"update": true}}), "org", "project", []models.InternalObject{candidate})
	if err != nil {
		t.Fatalf("BulkOverwriteObjects returned error: %v", err)
	}
	if result.Replaced != 1 || result.ChecksumMatched != 1 || result.Created != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if _, ok := db.Objects["source-did"]; ok {
		t.Fatal("checksum sibling should preserve the target DID")
	}
	got := db.Objects["target-did"]
	if got == nil || got.Name == nil || *got.Name != newName {
		t.Fatalf("source metadata did not replace target: %+v", got)
	}
}

func TestBulkOverwriteObjects_DoesNotMatchChecksumOutsideProject(t *testing.T) {
	resource, err := sycommon.ResourcePath("org", "project")
	if err != nil {
		t.Fatal(err)
	}
	sha := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Objects:     map[string]*drs.DrsObject{"other-project": {Id: "other-project", Checksums: []drs.Checksum{{Type: "sha256", Checksum: sha}}}},
		ObjectAuthz: map[string]map[string][]string{"other-project": {"org": {"other"}}},
	}}
	om := NewObjectManager(db, &capturingURLManager{})
	candidate := models.InternalObject{
		DrsObject:      drs.DrsObject{Id: "source-did", Checksums: []drs.Checksum{{Type: "sha256", Checksum: sha}}, ControlledAccess: &[]string{resource}},
		Authorizations: map[string][]string{"org": {"project"}},
	}
	result, err := om.BulkOverwriteObjects(context.Background(), "org", "project", []models.InternalObject{candidate})
	if err != nil {
		t.Fatalf("BulkOverwriteObjects returned error: %v", err)
	}
	if result.Created != 1 || result.ChecksumMatched != 0 || db.Objects["source-did"] == nil {
		t.Fatalf("checksum from another project must not be matched: %+v", result)
	}
}
