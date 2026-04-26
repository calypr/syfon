package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/client/bucketapi"
	"github.com/calypr/syfon/apigen/client/internalapi"
	syclient "github.com/calypr/syfon/client"
)

func TestSyfonListAndRemoveCommands(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	c, err := syclient.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	did := "11111111-1111-1111-1111-111111111111"
	fileName := "README.md"
	size := int64(123)
	auth := internalapi.AuthPathMap{"syfon": {"e2e": {"s3://syfon-bucket/path/README.md"}}}
	rec := internalapi.InternalRecord{
		Did:      did,
		Auth:     &auth,
		FileName: &fileName,
		Size:     &size,
	}
	if _, err := c.Index().Create(context.Background(), rec); err != nil {
		t.Fatalf("seed record: %v", err)
	}

	out, err := executeRootCommand(t, "--server", server.URL, "ls")
	if err != nil {
		t.Fatalf("ls failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, did) || !strings.Contains(out, "README.md") {
		t.Fatalf("ls output missing expected record: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "rm", "--did", did)
	if err != nil {
		t.Fatalf("rm failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "removed "+did) {
		t.Fatalf("unexpected rm output: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "ls")
	if err != nil {
		t.Fatalf("ls after rm failed: %v output=%s", err, out)
	}
	if strings.Contains(out, did) {
		t.Fatalf("expected did to be removed, got output: %s", out)
	}
}

func TestSyfonDownloadDefaultsToRecordFilename(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	tmp := t.TempDir()
	t.Chdir(tmp)

	srcPath := filepath.Join(server.StorageDir, "source.txt")
	srcData := []byte("download default filename test")
	if err := os.WriteFile(srcPath, srcData, 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}

	c, err := syclient.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	did := "22222222-2222-2222-2222-222222222222"
	// Store record with explicit filename and a storage-root URL so download can resolve locally.
	if err := c.Index().Upsert(context.Background(), did, "s3://syfon-bucket/source.txt", "README.md", int64(len(srcData)), "", map[string][]string{"syfon": {"e2e"}}); err != nil {
		t.Fatalf("seed record with file url: %v", err)
	}

	out, err := executeRootCommand(t, "--server", server.URL, "download", "--did", did)
	if err != nil {
		t.Fatalf("download failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "downloaded "+did+" -> README.md") {
		t.Fatalf("unexpected download output: %s", out)
	}

	gotPath := filepath.Join(tmp, "README.md")
	got, err := os.ReadFile(gotPath)
	if err != nil {
		t.Fatalf("expected README.md to be created: %v", err)
	}
	if string(got) != string(srcData) {
		t.Fatalf("downloaded data mismatch")
	}
}

func TestSyfonBucketListAndRemoveCommands(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	c, err := syclient.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Buckets().Put(context.Background(), bucketapi.PutBucketRequest{
		Bucket:       "test-bucket-cli",
		Provider:     stringPtr("s3"),
		Region:       stringPtr("us-east-1"),
		AccessKey:    stringPtr("ak"),
		SecretKey:    stringPtr("sk"),
		Organization: "syfon",
		ProjectId:    "e2e",
	}); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}

	out, err := executeRootCommand(t, "--server", server.URL, "bucket", "list")
	if err != nil {
		t.Fatalf("bucket list failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "test-bucket-cli") {
		t.Fatalf("bucket list missing bucket: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "bucket", "remove", "test-bucket-cli")
	if err != nil {
		t.Fatalf("bucket remove failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "bucket removed: test-bucket-cli") {
		t.Fatalf("unexpected bucket remove output: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "bucket", "list")
	if err != nil {
		t.Fatalf("bucket list after remove failed: %v output=%s", err, out)
	}
	if strings.Contains(out, "test-bucket-cli") {
		t.Fatalf("expected bucket to be removed, output=%s", out)
	}
}

func stringPtr(v string) *string { return &v }
