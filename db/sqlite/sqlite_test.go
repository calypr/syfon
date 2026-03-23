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
	if err := db.CreateObject(ctx, &core.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
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

func TestSqliteDB_GetObjectsByChecksum_WhenIDDiffers(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	checksum := "47454ac45ec9e9d88d76ba2dc8dff527ba6899a0f4189eb67dfcb2da0aa7d125"

	obj := &drs.DrsObject{
		Id:          "did-123",
		Size:        10,
		CreatedTime: time.Now(),
		UpdatedTime: time.Now(),
		Version:     "1.0",
		Name:        "oid-object",
		AccessMethods: []drs.AccessMethod{
			{
				Type: "s3",
				AccessUrl: drs.AccessMethodAccessUrl{
					Url: "s3://bucket/cbds/end_to_end_test/" + checksum,
				},
			},
		},
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: checksum},
		},
	}
	if err := db.CreateObject(ctx, &core.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	objs, err := db.GetObjectsByChecksum(ctx, checksum)
	if err != nil {
		t.Fatalf("GetObjectsByChecksum failed: %v", err)
	}
	if len(objs) != 1 {
		t.Fatalf("expected 1 object, got %d", len(objs))
	}
	if objs[0].Id != "did-123" {
		t.Fatalf("expected object id did-123, got %s", objs[0].Id)
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

	objects := []core.InternalObject{
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
	if err := db.CreateObject(ctx, &core.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
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

func TestSqliteDB_GetObjectsByChecksumsAndListByPrefix(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	now := time.Now()
	objects := []core.InternalObject{
		{
			DrsObject: drs.DrsObject{
				Id:          "sha-x",
				CreatedTime: now,
				UpdatedTime: now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "sha-x"}},
			},
			Authorizations: []string{"/programs/a/projects/b"},
		},
		{
			DrsObject: drs.DrsObject{
				Id:          "sha-y",
				CreatedTime: now,
				UpdatedTime: now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "sha-y"}},
			},
			Authorizations: []string{"/programs/a/projects/c"},
		},
	}
	if err := db.RegisterObjects(ctx, objects); err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}

	byChecksums, err := db.GetObjectsByChecksums(ctx, []string{"sha-x", "sha-y", "missing"})
	if err != nil {
		t.Fatalf("GetObjectsByChecksums failed: %v", err)
	}
	if len(byChecksums["sha-x"]) != 1 || byChecksums["sha-x"][0].Id != "sha-x" {
		t.Fatalf("unexpected checksum result for sha-x: %+v", byChecksums["sha-x"])
	}
	if len(byChecksums["missing"]) != 0 {
		t.Fatalf("expected empty results for missing checksum")
	}

	ids, err := db.ListObjectIDsByResourcePrefix(ctx, "/programs/a/projects/b")
	if err != nil {
		t.Fatalf("ListObjectIDsByResourcePrefix failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != "sha-x" {
		t.Fatalf("unexpected ids for prefix query: %+v", ids)
	}
}

func TestSqliteDB_BulkUpdateAccessMethods(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	now := time.Now()
	if err := db.RegisterObjects(ctx, []core.InternalObject{
		{
			DrsObject: drs.DrsObject{
				Id:          "obj-a",
				CreatedTime: now,
				UpdatedTime: now,
			},
		},
		{
			DrsObject: drs.DrsObject{
				Id:          "obj-b",
				CreatedTime: now,
				UpdatedTime: now,
			},
		},
	}); err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}

	err := db.BulkUpdateAccessMethods(ctx, map[string][]drs.AccessMethod{
		"obj-a": {
			{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/a"}},
		},
		"obj-b": {
			{Type: "s3", AccessUrl: drs.AccessMethodAccessUrl{Url: "s3://bucket/b"}},
		},
	})
	if err != nil {
		t.Fatalf("BulkUpdateAccessMethods failed: %v", err)
	}

	a, _ := db.GetObject(ctx, "obj-a")
	if len(a.AccessMethods) != 1 || a.AccessMethods[0].AccessUrl.Url != "s3://bucket/a" {
		t.Fatalf("unexpected access methods for obj-a: %+v", a.AccessMethods)
	}
}

func TestSqliteDB_GetServiceInfo(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	info, err := db.GetServiceInfo(ctx)
	if err != nil {
		t.Fatalf("GetServiceInfo failed: %v", err)
	}
	if info == nil || info.Name == "" {
		t.Fatalf("expected non-empty service info, got %+v", info)
	}
}

func TestSqliteDB_PendingLFSMetaLifecycle(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	now := time.Now().UTC()
	candidate := drs.DrsObjectCandidate{
		Name: "candidate",
		Size: 123,
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
	}

	if err := db.SavePendingLFSMeta(ctx, []core.PendingLFSMeta{
		{
			OID:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Candidate: candidate,
			CreatedAt: now,
			ExpiresAt: now.Add(5 * time.Minute),
		},
	}); err != nil {
		t.Fatalf("SavePendingLFSMeta failed: %v", err)
	}

	entry, err := db.PopPendingLFSMeta(ctx, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("PopPendingLFSMeta failed: %v", err)
	}
	if entry.Candidate.Name != "candidate" {
		t.Fatalf("unexpected candidate payload: %+v", entry.Candidate)
	}

	if _, err := db.PopPendingLFSMeta(ctx, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Fatalf("expected not found after pop")
	}
}

func TestSqliteDB_PendingLFSMetaPrunesExpired(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	now := time.Now().UTC()
	oid := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	candidate := drs.DrsObjectCandidate{
		Name: "expired",
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: oid},
		},
	}

	if err := db.SavePendingLFSMeta(ctx, []core.PendingLFSMeta{
		{
			OID:       oid,
			Candidate: candidate,
			CreatedAt: now.Add(-2 * time.Hour),
			ExpiresAt: now.Add(-1 * time.Hour),
		},
	}); err != nil {
		t.Fatalf("SavePendingLFSMeta failed: %v", err)
	}

	if _, err := db.PopPendingLFSMeta(ctx, oid); err == nil {
		t.Fatalf("expected not found for expired metadata")
	}
}

func TestSqliteDB_FileUsageMetrics(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	now := time.Now().UTC()
	oid := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	if err := db.CreateObject(ctx, &core.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          oid,
			Name:        "metrics-object",
			Size:        42,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
		},
	}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	if err := db.RecordFileUpload(ctx, oid); err != nil {
		t.Fatalf("RecordFileUpload failed: %v", err)
	}
	if err := db.RecordFileDownload(ctx, oid); err != nil {
		t.Fatalf("RecordFileDownload failed: %v", err)
	}
	if err := db.RecordFileDownload(ctx, oid); err != nil {
		t.Fatalf("RecordFileDownload failed: %v", err)
	}

	usage, err := db.GetFileUsage(ctx, oid)
	if err != nil {
		t.Fatalf("GetFileUsage failed: %v", err)
	}
	if usage.UploadCount != 1 || usage.DownloadCount != 2 {
		t.Fatalf("unexpected usage counters: %+v", usage)
	}
	if usage.LastAccessTime == nil {
		t.Fatalf("expected last access time to be set")
	}

	rows, err := db.ListFileUsage(ctx, 10, 0, nil)
	if err != nil {
		t.Fatalf("ListFileUsage failed: %v", err)
	}
	if len(rows) == 0 {
		t.Fatalf("expected at least one usage row")
	}

	summary, err := db.GetFileUsageSummary(ctx, nil)
	if err != nil {
		t.Fatalf("GetFileUsageSummary failed: %v", err)
	}
	if summary.TotalFiles == 0 || summary.TotalUploads == 0 || summary.TotalDownloads == 0 {
		t.Fatalf("unexpected summary: %+v", summary)
	}
}

func TestSqliteDB_FileUsageMetrics_MissingObjectQueuedAndFlushedOnCreate(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	oid := "missing-object"
	if err := db.RecordFileUpload(ctx, oid); err != nil {
		t.Fatalf("RecordFileUpload should queue for missing object, got: %v", err)
	}
	if err := db.RecordFileDownload(ctx, oid); err != nil {
		t.Fatalf("RecordFileDownload should queue for missing object, got: %v", err)
	}
	if _, err := db.GetFileUsage(ctx, oid); err == nil {
		t.Fatalf("expected not found for missing object usage")
	}

	now := time.Now().UTC()
	if err := db.CreateObject(ctx, &core.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          oid,
			Name:        "later-created",
			Size:        11,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
		},
	}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}
	usage, err := db.GetFileUsage(ctx, oid)
	if err != nil {
		t.Fatalf("GetFileUsage failed after create: %v", err)
	}
	if usage.UploadCount != 1 || usage.DownloadCount != 1 {
		t.Fatalf("expected queued usage to flush on create, got: %+v", usage)
	}
}
