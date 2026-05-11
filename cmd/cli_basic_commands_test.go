package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/client/bucketapi"
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
	storagePath := filepath.Join(server.StorageDir, "README.md")
	if err := os.WriteFile(storagePath, []byte("rm single scope"), 0o644); err != nil {
		t.Fatalf("seed storage object: %v", err)
	}
	if err := c.Index().Upsert(context.Background(), did, "s3://syfon-bucket/README.md", "README.md", 123, "", map[string][]string{"syfon": {"e2e"}}); err != nil {
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
	if !strings.Contains(out, "removed "+did+" and attempted storage purge") {
		t.Fatalf("unexpected rm output: %s", out)
	}

	out, err = executeRootCommand(t, "--server", server.URL, "ls")
	if err != nil {
		t.Fatalf("ls after rm failed: %v output=%s", err, out)
	}
	if strings.Contains(out, did) {
		t.Fatalf("expected did to be removed, got output: %s", out)
	}
	if _, err := os.Stat(storagePath); !os.IsNotExist(err) {
		t.Fatalf("expected backing storage to be removed, stat err=%v", err)
	}
}

func TestSyfonRemoveScopedControlledAccessOnly(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	c, err := syclient.New(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	did := "11111111-1111-1111-1111-222222222222"
	storagePath := filepath.Join(server.StorageDir, "scoped.txt")
	if err := os.WriteFile(storagePath, []byte("rm scoped"), 0o644); err != nil {
		t.Fatalf("seed storage object: %v", err)
	}
	if err := c.Index().Upsert(context.Background(), did, "s3://syfon-bucket/scoped.txt", "scoped.txt", 99, "", map[string][]string{
		"syfon": {"e2e"},
		"other": {"x"},
	}); err != nil {
		t.Fatalf("seed multi-scope record: %v", err)
	}

	out, err := executeRootCommand(t, "--server", server.URL, "rm", "--did", did, "--organization", "syfon", "--project", "e2e")
	if err != nil {
		t.Fatalf("rm failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "removed scoped access /organization/syfon/project/e2e from "+did) {
		t.Fatalf("unexpected rm output: %s", out)
	}

	rec, err := c.Index().Get(context.Background(), did)
	if err != nil {
		t.Fatalf("expected record to remain: %v", err)
	}
	controlled := derefCLIStringSlice(rec.ControlledAccess)
	if len(controlled) != 1 || controlled[0] != "/organization/other/project/x" {
		t.Fatalf("unexpected controlled access after scoped remove: %+v", controlled)
	}
	if _, err := os.Stat(storagePath); err != nil {
		t.Fatalf("expected backing storage to remain, stat err=%v", err)
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
	recordName := "nested/path/README.md"
	// Store record with explicit filename and a storage-root URL so download can resolve locally.
	if err := c.Index().Upsert(context.Background(), did, "s3://syfon-bucket/source.txt", recordName, int64(len(srcData)), "", map[string][]string{"syfon": {"e2e"}}); err != nil {
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
	if _, err := os.Stat(filepath.Join(tmp, "nested")); !os.IsNotExist(err) {
		t.Fatalf("expected nested path prefix to be ignored, stat err=%v", err)
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
		Provider:     stringPtr("file"),
		Region:       stringPtr("us-east-1"),
		AccessKey:    stringPtr("ak"),
		SecretKey:    stringPtr("sk"),
		Organization: "cli-tests",
		ProjectId:    "bucket-list",
	}); err != nil {
		t.Fatalf("seed bucket: %v", err)
	}
	if err := c.Index().Upsert(context.Background(), "bucket-visible-object", "s3://test-bucket-cli/visible.txt", "visible.txt", 7, "", map[string][]string{"cli-tests": {"bucket-list"}}); err != nil {
		t.Fatalf("seed visible object: %v", err)
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

func TestSyfonBucketAddCredentialAndScopesCommands(t *testing.T) {
	server := newSyfonTestServer(t)
	defer server.Close()

	out, err := executeRootCommand(t,
		"--server", server.URL,
		"bucket", "add", "test-bucket-cli",
		"--provider", "file",
		"--region", "us-east-1",
	)
	if err != nil {
		t.Fatalf("bucket add failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "bucket credential configured: test-bucket-cli") {
		t.Fatalf("unexpected bucket add output: %s", out)
	}

	out, err = executeRootCommand(t,
		"--server", server.URL,
		"bucket", "add-organization", "cli-tests",
		"--path", "gs://test-bucket-cli/program-root",
	)
	if err != nil {
		t.Fatalf("bucket add-organization failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "bucket organization scope configured: bucket=test-bucket-cli org=cli-tests") {
		t.Fatalf("unexpected bucket add-organization output: %s", out)
	}

	out, err = executeRootCommand(t,
		"--server", server.URL,
		"bucket", "add-project", "cli-tests", "bucket-cli",
		"--path", "gs://test-bucket-cli/program-root/project-subpath",
	)
	if err != nil {
		t.Fatalf("bucket add-project failed: %v output=%s", err, out)
	}
	if !strings.Contains(out, "bucket project scope configured: bucket=test-bucket-cli org=cli-tests project=bucket-cli") {
		t.Fatalf("unexpected bucket add-project output: %s", out)
	}
}

func stringPtr(v string) *string { return &v }

func derefCLIStringSlice(in *[]string) []string {
	if in == nil {
		return nil
	}
	return *in
}
