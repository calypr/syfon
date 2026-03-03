package sqlite

import (
	"context"
	"testing"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
)

func TestSqliteDB_CRUD(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	obj := &drs.DrsObject{
		Id:          "abc",
		Size:        123,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
		Version:     "1.0",
		Name:        "testing",
		AccessMethods: []drs.AccessMethod{
			{
				Type: "s3",
				AccessUrl: drs.AccessMethodAccessUrl{
					Url: "s3://bucket/key",
				},
			},
		},
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: "abc"},
		},
	}

	// Create
	if err := db.CreateObject(ctx, obj, []string{}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	// Get
	fetched, err := db.GetObject(ctx, "abc")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if fetched.Size != obj.Size {
		t.Errorf("expected size %d, got %d", obj.Size, fetched.Size)
	}
	if len(fetched.AccessMethods) != 1 {
		t.Errorf("expected 1 access method, got %d", len(fetched.AccessMethods))
	}

	// Get by Checksum
	objs, err := db.GetObjectsByChecksum(ctx, "abc")
	if err != nil {
		t.Fatalf("GetObjectsByChecksum failed: %v", err)
	}
	if len(objs) != 1 || objs[0].Id != "abc" {
		t.Errorf("expected 1 object with id abc, got %v", objs)
	}

	// Delete
	if err := db.DeleteObject(ctx, "abc"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	// Verify Deleted
	_, err = db.GetObject(ctx, "abc")
	if err == nil {
		t.Fatal("expected error getting deleted object, got nil")
	}
}

func TestSqliteDB_S3Credentials(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	cred := &core.S3Credential{
		Bucket:    "test-bucket",
		Region:    "us-east-1",
		AccessKey: "key",
		SecretKey: "secret",
		Endpoint:  "http://localhost:9000",
	}

	if err := db.SaveS3Credential(ctx, cred); err != nil {
		t.Fatalf("SaveS3Credential failed: %v", err)
	}

	fetched, err := db.GetS3Credential(ctx, "test-bucket")
	if err != nil {
		t.Fatalf("GetS3Credential failed: %v", err)
	}
	if fetched.AccessKey != cred.AccessKey {
		t.Errorf("expected key %s, got %s", cred.AccessKey, fetched.AccessKey)
	}

	list, err := db.ListS3Credentials(ctx)
	if err != nil {
		t.Fatalf("ListS3Credentials failed: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 cred, got %d", len(list))
	}

	if err := db.DeleteS3Credential(ctx, "test-bucket"); err != nil {
		t.Fatalf("DeleteS3Credential failed: %v", err)
	}
}

func TestSqliteDB_BulkOperations(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	objects := []core.DrsObjectWithAuthz{
		{DrsObject: drs.DrsObject{Id: "bulk-1", Size: 10}},
		{DrsObject: drs.DrsObject{Id: "bulk-2", Size: 20}},
	}

	if err := db.RegisterObjects(ctx, objects); err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}

	fetched, _ := db.GetBulkObjects(ctx, []string{"bulk-1", "bulk-2"})
	if len(fetched) != 2 {
		t.Errorf("expected 2 objects, got %d", len(fetched))
	}

	if err := db.BulkDeleteObjects(ctx, []string{"bulk-1", "bulk-2"}); err != nil {
		t.Fatalf("BulkDeleteObjects failed: %v", err)
	}
}

func TestSqliteDB_UpdateAccessMethods(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	obj := &drs.DrsObject{Id: "update-me"}
	if err := db.CreateObject(ctx, obj, []string{}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	newMethods := []drs.AccessMethod{
		{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://new/path"}},
	}

	if err := db.UpdateObjectAccessMethods(ctx, "update-me", newMethods); err != nil {
		t.Fatalf("UpdateObjectAccessMethods failed: %v", err)
	}

	fetched, err := db.GetObject(ctx, "update-me")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if len(fetched.AccessMethods) != 1 || fetched.AccessMethods[0].AccessUrl.Url != "s3://new/path" {
		t.Errorf("expected updated access method, got %v", fetched.AccessMethods)
	}
}
