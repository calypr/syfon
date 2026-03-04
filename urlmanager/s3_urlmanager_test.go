package urlmanager

import (
	"context"
	"strings"
	"testing"

	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/db/sqlite"
)

func TestS3UrlManager_SignURL(t *testing.T) {
	ctx := context.Background()
	// Use in-memory SQLite for testing
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}

	// Create and save a credential
	cred := &core.S3Credential{
		Bucket:    "my-bucket",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
	}
	// Depending on how SaveS3Credential is defined, it might need a context.
	// db.NewInMemoryDB likely returns a *InMemoryDB which implements DatabaseInterface.
	if err := database.SaveS3Credential(ctx, cred); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewS3UrlManager(database)

	urlStr := "s3://my-bucket/my-obj"
	signedURL, err := manager.SignURL(ctx, "resource-1", urlStr, SignOptions{})
	if err != nil {
		t.Fatalf("SignURL failed: %v", err)
	}

	// The exact formatted URL might depend on the SDK/Provider, but it should contain the bucket and key.
	// For standard S3: https://my-bucket.s3.us-east-1.amazonaws.com/my-obj?...
	if !strings.Contains(signedURL, "my-bucket") || !strings.Contains(signedURL, "my-obj") {
		t.Errorf("expected signed URL to contain bucket and key, got: %s", signedURL)
	}

	if !strings.Contains(signedURL, "X-Amz-Signature") {
		t.Errorf("expected signed URL to contain signature, got: %s", signedURL)
	}

	// Also test Upload URL
	uploadURL, err := manager.SignUploadURL(ctx, "resource-1", urlStr, SignOptions{})
	if err != nil {
		t.Fatalf("SignUploadURL failed: %v", err)
	}
	if !strings.Contains(uploadURL, "my-bucket") || !strings.Contains(uploadURL, "X-Amz-Signature") {
		t.Errorf("expected upload URL to contain bucket and signature, got: %s", uploadURL)
	}
}

func TestS3UrlManager_InvalidScheme(t *testing.T) {
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	manager := NewS3UrlManager(database)

	if _, err := manager.SignURL(ctx, "id", "https://example.org/file", SignOptions{}); err == nil {
		t.Fatal("expected SignURL to fail for non-s3 scheme")
	}
	if _, err := manager.SignUploadURL(ctx, "id", "https://example.org/file", SignOptions{}); err == nil {
		t.Fatal("expected SignUploadURL to fail for non-s3 scheme")
	}
}

func TestS3UrlManager_MultipartMethods(t *testing.T) {
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
		Endpoint:  "http://127.0.0.1:1",
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewS3UrlManager(database)

	// Presigning upload part is local and should succeed without calling remote services.
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
	if err := manager.CompleteMultipartUpload(ctx, "mp-bucket", "obj", "upload-id", []MultipartPart{{PartNumber: 1, ETag: "etag"}}); err == nil {
		t.Fatal("expected CompleteMultipartUpload to fail against unreachable endpoint")
	}
}
