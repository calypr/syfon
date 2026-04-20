package sqlite

import (
	"context"
	"errors"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/crypto"
	"github.com/calypr/syfon/internal/models"
	"testing"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
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
		UpdatedTime: func() *time.Time { t := time.Now(); return &t }(),
		Version:     common.Ptr("1.0"),
		Name:        common.Ptr("testing"),
		AccessMethods: &[]drs.AccessMethod{
			{
				Type: drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/key"},
			},
		},
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: "abc"},
		},
	}

	// Create
	if err := db.CreateObject(ctx, &models.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
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
	if fetched.AccessMethods == nil || len(*fetched.AccessMethods) != 1 {
		t.Errorf("expected 1 access method, got %v", fetched.AccessMethods)
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
		UpdatedTime: func() *time.Time { t := time.Now(); return &t }(),
		Version:     common.Ptr("1.0"),
		Name:        common.Ptr("oid-object"),
		AccessMethods: &[]drs.AccessMethod{
			{
				Type: drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/cbds/end_to_end_test/" + checksum},
			},
		},
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: checksum},
		},
	}
	if err := db.CreateObject(ctx, &models.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
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

func TestSqliteDB_ObjectAliasLifecycle(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	canonicalID := "11111111-1111-4111-8111-111111111111"
	aliasID := "22222222-2222-4222-8222-222222222222"
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Now().UTC()

	if err := db.CreateObject(ctx, &models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          canonicalID,
			CreatedTime: now,
			UpdatedTime: &now,
			Checksums:   []drs.Checksum{{Type: "sha256", Checksum: checksum}},
			AccessMethods: &[]drs.AccessMethod{
				{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://bucket/path/object"}},
			},
		},
		Authorizations: []string{"/programs/a/projects/b"},
	}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	if err := db.CreateObjectAlias(ctx, aliasID, canonicalID); err != nil {
		t.Fatalf("CreateObjectAlias failed: %v", err)
	}

	resolved, err := db.ResolveObjectAlias(ctx, aliasID)
	if err != nil {
		t.Fatalf("ResolveObjectAlias failed: %v", err)
	}
	if resolved != canonicalID {
		t.Fatalf("expected canonical id %s, got %s", canonicalID, resolved)
	}

	aliased, err := db.GetObject(ctx, aliasID)
	if err != nil {
		t.Fatalf("GetObject(alias) failed: %v", err)
	}
	if aliased.Id != aliasID {
		t.Fatalf("expected alias id %s, got %s", aliasID, aliased.Id)
	}
	if len(aliased.Checksums) != 1 || aliased.Checksums[0].Checksum != checksum {
		t.Fatalf("expected checksum to resolve through alias, got %+v", aliased.Checksums)
	}

	byChecksum, err := db.GetObjectsByChecksum(ctx, checksum)
	if err != nil {
		t.Fatalf("GetObjectsByChecksum failed: %v", err)
	}
	if len(byChecksum) != 1 || byChecksum[0].Id != canonicalID {
		t.Fatalf("expected exactly one canonical record for checksum, got %+v", byChecksum)
	}

	if err := db.DeleteObjectAlias(ctx, aliasID); err != nil {
		t.Fatalf("DeleteObjectAlias(alias) failed: %v", err)
	}
	if _, err := db.ResolveObjectAlias(ctx, aliasID); err == nil {
		t.Fatal("expected alias to be deleted")
	}
	if _, err := db.GetObject(ctx, canonicalID); err != nil {
		t.Fatalf("expected canonical object to remain, got error: %v", err)
	}
}

func TestSqliteDB_S3Credentials(t *testing.T) {
	t.Setenv(crypto.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	cred := &models.S3Credential{
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

func TestSqliteDB_S3Credentials_EncryptedAtRest(t *testing.T) {
	t.Setenv(crypto.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	cred := &models.S3Credential{
		Bucket:    "enc-bucket",
		Region:    "us-east-1",
		AccessKey: "plain-ak",
		SecretKey: "plain-sk",
		Endpoint:  "http://localhost:9000",
	}
	if err := db.SaveS3Credential(ctx, cred); err != nil {
		t.Fatalf("SaveS3Credential failed: %v", err)
	}

	var storedAK, storedSK string
	if err := db.db.QueryRowContext(ctx, "SELECT access_key, secret_key FROM s3_credential WHERE bucket = ?", "enc-bucket").Scan(&storedAK, &storedSK); err != nil {
		t.Fatalf("raw select failed: %v", err)
	}
	if storedAK == "plain-ak" || storedSK == "plain-sk" {
		t.Fatalf("expected encrypted values at rest, got access=%q secret=%q", storedAK, storedSK)
	}

	got, err := db.GetS3Credential(ctx, "enc-bucket")
	if err != nil {
		t.Fatalf("GetS3Credential failed: %v", err)
	}
	if got.AccessKey != "plain-ak" || got.SecretKey != "plain-sk" {
		t.Fatalf("expected decrypted values, got %+v", got)
	}
}

func TestSqliteDB_BulkOperations(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	objects := []models.InternalObject{
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
	if err := db.CreateObject(ctx, &models.InternalObject{DrsObject: *obj, Authorizations: []string{}}); err != nil {
		t.Fatalf("CreateObject failed: %v", err)
	}

	newMethods := []drs.AccessMethod{
		{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: "s3://new/path"}},
	}

	if err := db.UpdateObjectAccessMethods(ctx, "update-me", newMethods); err != nil {
		t.Fatalf("UpdateObjectAccessMethods failed: %v", err)
	}

	fetched, err := db.GetObject(ctx, "update-me")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	if fetched.AccessMethods == nil || len(*fetched.AccessMethods) != 1 || (*fetched.AccessMethods)[0].AccessUrl == nil || (*fetched.AccessMethods)[0].AccessUrl.Url != "s3://new/path" {
		t.Errorf("expected updated access method, got %v", fetched.AccessMethods)
	}
}

func TestSqliteDB_GetObjectsByChecksumsAndListByPrefix(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	now := time.Now()
	objects := []models.InternalObject{
		{
			DrsObject: drs.DrsObject{
				Id:          "sha-x",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "sha-x"}},
			},
			Authorizations: []string{"/programs/a/projects/b"},
		},
		{
			DrsObject: drs.DrsObject{
				Id:          "sha-y",
				CreatedTime: now,
				UpdatedTime: &now,
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

func TestSqliteDB_ListObjectIDsByResourcePrefixRootIncludesUnscoped(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")
	now := time.Now()

	if err := db.RegisterObjects(ctx, []models.InternalObject{
		{
			DrsObject: drs.DrsObject{
				Id:          "scoped",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "scoped"}},
			},
			Authorizations: []string{"/programs/a/projects/b"},
		},
		{
			DrsObject: drs.DrsObject{
				Id:          "unscoped",
				CreatedTime: now,
				UpdatedTime: &now,
				Checksums:   []drs.Checksum{{Type: "sha256", Checksum: "unscoped"}},
			},
		},
	}); err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}

	ids, err := db.ListObjectIDsByResourcePrefix(ctx, "/")
	if err != nil {
		t.Fatalf("ListObjectIDsByResourcePrefix root failed: %v", err)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if !seen["scoped"] || !seen["unscoped"] {
		t.Fatalf("expected scoped and unscoped ids, got %+v", ids)
	}
}

func TestSqliteDB_BulkUpdateAccessMethods(t *testing.T) {
	ctx := context.Background()
	db, _ := NewSqliteDB(":memory:")

	now := time.Now()
	if err := db.RegisterObjects(ctx, []models.InternalObject{
		{
			DrsObject: drs.DrsObject{
				Id:          "obj-a",
				CreatedTime: now,
				UpdatedTime: &now,
			},
		},
		{
			DrsObject: drs.DrsObject{
				Id:          "obj-b",
				CreatedTime: now,
				UpdatedTime: &now,
			},
		},
	}); err != nil {
		t.Fatalf("RegisterObjects failed: %v", err)
	}

	err := db.BulkUpdateAccessMethods(ctx, map[string][]drs.AccessMethod{
		"obj-a": {
			{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/a"}},
		},
		"obj-b": {
			{Type: drs.AccessMethodTypeS3, AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: "s3://bucket/b"}},
		},
	})
	if err != nil {
		t.Fatalf("BulkUpdateAccessMethods failed: %v", err)
	}

	a, _ := db.GetObject(ctx, "obj-a")
	if a.AccessMethods == nil || len(*a.AccessMethods) != 1 || (*a.AccessMethods)[0].AccessUrl.Url != "s3://bucket/a" {
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
		Name: common.Ptr("candidate"),
		Size: 123,
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
	}

	if err := db.SavePendingLFSMeta(ctx, []models.PendingLFSMeta{
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
	if common.StringVal(entry.Candidate.Name) != "candidate" {
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
		Name: common.Ptr("expired"),
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: oid},
		},
	}

	if err := db.SavePendingLFSMeta(ctx, []models.PendingLFSMeta{
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
	if err := db.CreateObject(ctx, &models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          oid,
			Name:        common.Ptr("metrics-object"),
			Size:        42,
			CreatedTime: now,
			UpdatedTime: &now,
			Version:     common.Ptr("1"),
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
	if err := db.CreateObject(ctx, &models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          oid,
			Name:        common.Ptr("later-created"),
			Size:        11,
			CreatedTime: now,
			UpdatedTime: &now,
			Version:     common.Ptr("1"),
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

func TestSqliteDB_BucketScopeLifecycle(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	if err := db.CreateBucketScope(ctx, nil); err == nil {
		t.Fatalf("expected validation error for nil scope")
	}

	scope := &models.BucketScope{
		Organization: "calypr",
		ProjectID:    "proj-a",
		Bucket:       "bucket-a",
		PathPrefix:   "/data/a/",
	}
	if err := db.CreateBucketScope(ctx, scope); err != nil {
		t.Fatalf("CreateBucketScope failed: %v", err)
	}

	got, err := db.GetBucketScope(ctx, "calypr", "proj-a")
	if err != nil {
		t.Fatalf("GetBucketScope failed: %v", err)
	}
	if got.Bucket != "bucket-a" || got.PathPrefix != "data/a" {
		t.Fatalf("unexpected scope: %+v", got)
	}

	if err := db.CreateBucketScope(ctx, &models.BucketScope{
		Organization: "calypr",
		ProjectID:    "proj-a",
		Bucket:       "bucket-a",
		PathPrefix:   "data/a",
	}); err != nil {
		t.Fatalf("idempotent create should succeed, got: %v", err)
	}

	if err := db.CreateBucketScope(ctx, &models.BucketScope{
		Organization: "calypr",
		ProjectID:    "proj-a",
		Bucket:       "bucket-b",
		PathPrefix:   "data/b",
	}); !errors.Is(err, common.ErrConflict) {
		t.Fatalf("expected ErrConflict for remap, got: %v", err)
	}

	all, err := db.ListBucketScopes(ctx)
	if err != nil {
		t.Fatalf("ListBucketScopes failed: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("expected 1 scope, got %d", len(all))
	}

	_, err = db.GetBucketScope(ctx, "calypr", "missing")
	if !errors.Is(err, common.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing scope, got: %v", err)
	}
}

func TestSqliteDB_GetPendingLFSMeta(t *testing.T) {
	ctx := context.Background()
	db, err := NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}

	now := time.Now().UTC()
	oid := "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	candidate := drs.DrsObjectCandidate{
		Name: common.Ptr("candidate-get"),
		Checksums: []drs.Checksum{
			{Type: "sha256", Checksum: oid},
		},
	}

	if err := db.SavePendingLFSMeta(ctx, []models.PendingLFSMeta{
		{
			OID:       oid,
			Candidate: candidate,
			CreatedAt: now,
			ExpiresAt: now.Add(10 * time.Minute),
		},
	}); err != nil {
		t.Fatalf("SavePendingLFSMeta failed: %v", err)
	}

	got, err := db.GetPendingLFSMeta(ctx, oid)
	if err != nil {
		t.Fatalf("GetPendingLFSMeta failed: %v", err)
	}
	if common.StringVal(got.Candidate.Name) != "candidate-get" {
		t.Fatalf("unexpected pending metadata: %+v", got)
	}

	_, err = db.GetPendingLFSMeta(ctx, "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	if !errors.Is(err, common.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing pending metadata, got: %v", err)
	}
}
