package urlmanager

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestS3UrlManager_SignURL(t *testing.T) {
	// Set dummy environment variables for AWS credentials
	os.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Setenv("AWS_REGION", "us-east-1")
	defer func() {
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
		os.Unsetenv("AWS_REGION")
	}()

	ctx := context.Background()
	manager, err := NewS3UrlManager(ctx)
	if err != nil {
		t.Fatalf("failed to create S3UrlManager: %v", err)
	}

	urlStr := "s3://my-bucket/my-obj"
	signedURL, err := manager.SignURL(ctx, "resource-1", urlStr)
	if err != nil {
		t.Fatalf("SignURL failed: %v", err)
	}

	if !strings.Contains(signedURL, "https://my-bucket.s3.us-east-1.amazonaws.com/my-obj") {
		t.Errorf("expected signed URL to contain standard S3 endpoint for bucket/key, got: %s", signedURL)
	}

	if !strings.Contains(signedURL, "X-Amz-Signature") {
		t.Errorf("expected signed URL to contain signature, got: %s", signedURL)
	}
}
