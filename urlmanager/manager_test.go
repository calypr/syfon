package urlmanager

import (
	"context"
	"strings"
	"testing"

	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/db/sqlite"
)

func TestManager_SignURL(t *testing.T) {
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
