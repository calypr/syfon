package urlmanager

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/db/sqlite"
	"gocloud.dev/blob/memblob"
)

func TestManager_SignURL(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}

	cred := &core.S3Credential{
		Bucket:    "my-bucket",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
	}
	if err := database.SaveS3Credential(ctx, cred); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})

	urlStr := "s3://my-bucket/my-obj"
	signedURL, err := manager.SignURL(ctx, "resource-1", urlStr, SignOptions{})
	if err != nil {
		t.Fatalf("SignURL failed: %v", err)
	}

	if !strings.Contains(signedURL, "my-bucket") || !strings.Contains(signedURL, "my-obj") {
		t.Errorf("expected signed URL to contain bucket and key, got: %s", signedURL)
	}

	if !strings.Contains(signedURL, "X-Amz-Signature") {
		t.Errorf("expected signed URL to contain signature, got: %s", signedURL)
	}

	uploadURL, err := manager.SignUploadURL(ctx, "resource-1", urlStr, SignOptions{})
	if err != nil {
		t.Fatalf("SignUploadURL failed: %v", err)
	}
	if !strings.Contains(uploadURL, "my-bucket") || !strings.Contains(uploadURL, "X-Amz-Signature") {
		t.Errorf("expected upload URL to contain bucket and signature, got: %s", uploadURL)
	}
}

func TestManager_FileScheme(t *testing.T) {
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})

	// file:// should work without DB credentials
	urlStr := "file:///tmp/test.txt"
	signedURL, err := manager.SignURL(ctx, "", urlStr, SignOptions{})
	if err != nil {
		t.Fatalf("SignURL failed for file scheme: %v", err)
	}
	if !strings.Contains(signedURL, "test.txt") {
		t.Errorf("expected file URL to contain filename, got: %s", signedURL)
	}
}

func TestManager_MultipartMethods(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "mp-bucket",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
		Endpoint:  "http://127.0.0.1:1", // Unreachable endpoint for init/complete tests
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})

	// Presigning upload part is local.
	partURL, err := manager.SignMultipartPart(ctx, "mp-bucket", "obj", "upload-id", 1)
	if err != nil {
		t.Fatalf("SignMultipartPart failed: %v", err)
	}
	if !strings.Contains(partURL, "partNumber=1") {
		t.Fatalf("expected part URL with part number, got %s", partURL)
	}

	// Init/complete hit S3 API; against an unreachable endpoint they should fail cleanly.
	if _, err := manager.InitMultipartUpload(ctx, "mp-bucket", "obj"); err == nil {
		t.Fatal("expected InitMultipartUpload to fail against unreachable endpoint")
	}
}

func TestManager_ResolveFallbackFromAccessIDToURLBucket(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "cbds",
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})

	signedURL, err := manager.SignURL(ctx, "s3", "s3://cbds/path/to/object", SignOptions{})
	if err != nil {
		t.Fatalf("SignURL failed: %v", err)
	}
	if !strings.Contains(signedURL, "cbds") || !strings.Contains(signedURL, "X-Amz-Signature") {
		t.Fatalf("unexpected signed url: %s", signedURL)
	}
}

func TestCompleteMultipartByStitching(t *testing.T) {
	ctx := context.Background()
	bucket := memblob.OpenBucket(nil)
	key := "test/object.bin"
	uploadID := "upload-123"

	part1 := multipartPartObjectKey(key, uploadID, 1)
	part2 := multipartPartObjectKey(key, uploadID, 2)

	w1, err := bucket.NewWriter(ctx, part1, nil)
	if err != nil {
		t.Fatalf("open part1 writer: %v", err)
	}
	if _, err := w1.Write([]byte("hello ")); err != nil {
		t.Fatalf("write part1: %v", err)
	}
	if err := w1.Close(); err != nil {
		t.Fatalf("close part1: %v", err)
	}

	w2, err := bucket.NewWriter(ctx, part2, nil)
	if err != nil {
		t.Fatalf("open part2 writer: %v", err)
	}
	if _, err := w2.Write([]byte("world")); err != nil {
		t.Fatalf("write part2: %v", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatalf("close part2: %v", err)
	}

	if err := completeMultipartByStitching(ctx, bucket, key, uploadID, []MultipartPart{
		{PartNumber: 2, ETag: "e2"},
		{PartNumber: 1, ETag: "e1"},
	}); err != nil {
		t.Fatalf("completeMultipartByStitching failed: %v", err)
	}

	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		t.Fatalf("open stitched object: %v", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read stitched object: %v", err)
	}
	if got := string(b); got != "hello world" {
		t.Fatalf("unexpected stitched object content: %q", got)
	}

	if _, err := bucket.NewReader(ctx, part1, nil); err == nil {
		t.Fatalf("expected part1 to be cleaned up")
	}
	if _, err := bucket.NewReader(ctx, part2, nil); err == nil {
		t.Fatalf("expected part2 to be cleaned up")
	}
}
