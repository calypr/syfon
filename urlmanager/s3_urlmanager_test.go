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
