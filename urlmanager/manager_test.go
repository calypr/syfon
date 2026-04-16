package urlmanager

import (
	"context"
	"errors"
	"io"
	"net/url"
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
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			t.Logf("warning: failed to close stitched object reader: %v", closeErr)
		}
	}()
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

func TestManager_AzureSignURLAndUploadURL(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "az-container",
		Provider:  "azure",
		AccessKey: "devstoreaccount1",
		SecretKey: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
		Endpoint:  "http://127.0.0.1:10000/devstoreaccount1",
	}); err != nil {
		t.Fatalf("failed to save azure credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})

	signedURL, err := manager.SignURL(ctx, "", "azblob://az-container/path/to/object.bin", SignOptions{})
	if err != nil {
		t.Fatalf("SignURL failed: %v", err)
	}
	parsedGet, err := url.Parse(signedURL)
	if err != nil {
		t.Fatalf("parse signed get url: %v", err)
	}
	if got, want := parsedGet.Path, "/devstoreaccount1/az-container/path/to/object.bin"; got != want {
		t.Fatalf("unexpected signed get path: got %q want %q", got, want)
	}
	if parsedGet.Query().Get("sig") == "" {
		t.Fatalf("expected signed get URL to include sig query parameter: %s", signedURL)
	}

	signedUploadURL, err := manager.SignUploadURL(ctx, "", "azblob://az-container/path/to/object.bin", SignOptions{})
	if err != nil {
		t.Fatalf("SignUploadURL failed: %v", err)
	}
	parsedPut, err := url.Parse(signedUploadURL)
	if err != nil {
		t.Fatalf("parse signed put url: %v", err)
	}
	if got, want := parsedPut.Path, "/devstoreaccount1/az-container/path/to/object.bin"; got != want {
		t.Fatalf("unexpected signed put path: got %q want %q", got, want)
	}
	if parsedPut.Query().Get("sig") == "" {
		t.Fatalf("expected signed put URL to include sig query parameter: %s", signedUploadURL)
	}
}

func TestManager_SignDownloadPart_S3(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "download-bucket",
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})
	signed, err := manager.SignDownloadPart(ctx, "", "s3://download-bucket/path/to/object.bin", 0, 9, SignOptions{})
	if err != nil {
		t.Fatalf("SignDownloadPart failed: %v", err)
	}
	if !strings.Contains(signed, "X-Amz-Signature") {
		t.Fatalf("expected presigned URL, got %s", signed)
	}
}

func TestManager_CompleteMultipartUpload_FailsOnUnreachableS3(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "mp-complete-bucket",
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "test-key-id",
		SecretKey: "test-secret-key",
		Endpoint:  "http://127.0.0.1:1",
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	manager := NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900})
	err = manager.CompleteMultipartUpload(ctx, "mp-complete-bucket", "obj", "upload-id", []MultipartPart{{PartNumber: 1, ETag: "etag1"}})
	if err == nil {
		t.Fatal("expected CompleteMultipartUpload to fail against unreachable endpoint")
	}
}

func TestIsSigningNotSupported(t *testing.T) {
	if isSigningNotSupported(nil) {
		t.Fatal("nil error should not be signing-not-supported")
	}
	if isSigningNotSupported(context.Canceled) {
		t.Fatal("context.Canceled should not match signing-not-supported")
	}
	if !isSigningNotSupported(errors.New("driver signing not supported")) {
		t.Fatal("expected helper to match 'not supported' text")
	}
	if !isSigningNotSupported(errors.New("unimplemented")) {
		t.Fatal("expected helper to match 'unimplemented' text")
	}
}
