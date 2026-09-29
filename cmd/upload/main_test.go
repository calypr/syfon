package upload

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/bucketapi"
	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
)

type didLookupStub struct {
	obj drsapi.DrsObject
	err error
}

func (s didLookupStub) GetObject(context.Context, string) (drsapi.DrsObject, error) {
	if s.err != nil {
		return drsapi.DrsObject{}, s.err
	}
	return s.obj, nil
}

func TestResolveUploadBucketForScopePrefersExactProjectMatch(t *testing.T) {
	bucket, err := resolveUploadBucketForScope(bucketapi.BucketsResponse{
		S3BUCKETS: map[string]bucketapi.BucketMetadata{
			"org-bucket":     {Programs: &[]string{"/organization/cbds"}},
			"project-bucket": {Programs: &[]string{"/organization/cbds/project/proj1"}},
		},
	}, "cbds", "proj1")
	if err != nil {
		t.Fatalf("resolveUploadBucketForScope returned error: %v", err)
	}
	if bucket != "project-bucket" {
		t.Fatalf("expected project-bucket, got %q", bucket)
	}
}

func TestResolveUploadBucketForScopeFallsBackToOrganizationScope(t *testing.T) {
	bucket, err := resolveUploadBucketForScope(bucketapi.BucketsResponse{
		S3BUCKETS: map[string]bucketapi.BucketMetadata{
			"org-bucket": {Programs: &[]string{"/organization/cbds"}},
		},
	}, "cbds", "proj1")
	if err != nil {
		t.Fatalf("resolveUploadBucketForScope returned error: %v", err)
	}
	if bucket != "org-bucket" {
		t.Fatalf("expected org-bucket, got %q", bucket)
	}
}

func TestResolveUploadBucketForScopeRejectsAmbiguousOrganizationScope(t *testing.T) {
	_, err := resolveUploadBucketForScope(bucketapi.BucketsResponse{
		S3BUCKETS: map[string]bucketapi.BucketMetadata{
			"bucket-a": {Programs: &[]string{"/organization/cbds"}},
			"bucket-b": {Programs: &[]string{"/organization/cbds"}},
		},
	}, "cbds", "")
	if err == nil || !strings.Contains(err.Error(), "maps to multiple buckets") {
		t.Fatalf("expected ambiguity error, got %v", err)
	}
}

func TestResolveUploadBucketForScopeRejectsMissingScope(t *testing.T) {
	_, err := resolveUploadBucketForScope(bucketapi.BucketsResponse{
		S3BUCKETS: map[string]bucketapi.BucketMetadata{
			"other-bucket": {Programs: &[]string{"/organization/other"}},
		},
	}, "cbds", "proj1")
	if err == nil || !strings.Contains(err.Error(), "no bucket configured") {
		t.Fatalf("expected missing scope error, got %v", err)
	}
}

func TestEnsureWritableDIDRequiresOverwriteForExistingObject(t *testing.T) {
	stub := &didLookupStub{obj: drsapi.DrsObject{Id: "did-1"}}
	_, err := ensureWritableDID(context.Background(), stub, "did-1", false)
	if err == nil || !strings.Contains(err.Error(), "--overwrite") {
		t.Fatalf("expected overwrite guidance, got %v", err)
	}
}

func TestEnsureWritableDIDAllowsOverwriteForExistingObject(t *testing.T) {
	oldSHA := strings.Repeat("a", 64)
	controlled := []string{"/programs/demo/projects/test"}
	stub := &didLookupStub{obj: drsapi.DrsObject{Id: "did-1", Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: oldSHA}}, ControlledAccess: &controlled}}
	info, err := ensureWritableDID(context.Background(), stub, "did-1", true)
	if err != nil {
		t.Fatalf("expected overwrite to allow existing DID, got %v", err)
	}
	if !strings.Contains(info.Warning, "already exists") {
		t.Fatalf("expected overwrite warning, got %q", info.Warning)
	}
	if info.ExpectedOldSHA != oldSHA {
		t.Fatalf("expected precondition %q, got %q", oldSHA, info.ExpectedOldSHA)
	}
	if info.Existing == nil || info.Existing.Id != "did-1" {
		t.Fatalf("expected existing object in overwrite info, got %+v", info.Existing)
	}
}

func TestEnsureWritableDIDRejectsOverwriteWithoutSHA256(t *testing.T) {
	_, err := ensureWritableDID(context.Background(), &didLookupStub{obj: drsapi.DrsObject{Id: "did-1"}}, "did-1", true)
	if err == nil || !strings.Contains(err.Error(), "no valid SHA-256") {
		t.Fatalf("expected safe replacement precondition error, got %v", err)
	}
}

func TestEnsureWritableDIDAllowsMissingObject(t *testing.T) {
	info, err := ensureWritableDID(context.Background(), &didLookupStub{err: errorapi.ErrNotFound}, "did-1", false)
	if err != nil || info.Warning != "" || info.ExpectedOldSHA != "" {
		t.Fatalf("expected missing DID to be writable without warning or precondition, got info=%+v err=%v", info, err)
	}
}

func TestEnsureWritableDIDPropagatesLookupFailure(t *testing.T) {
	_, err := ensureWritableDID(context.Background(), &didLookupStub{err: errors.New("boom")}, "did-1", false)
	if err == nil || !strings.Contains(err.Error(), "check existing DID") {
		t.Fatalf("expected lookup failure, got %v", err)
	}
}

func TestUploadRecordPathPreservesRelativeHierarchy(t *testing.T) {
	got, err := uploadRecordPath("data/tcga.tumor.hugo.tsv")
	if err != nil {
		t.Fatalf("uploadRecordPath returned error: %v", err)
	}
	if got != "data/tcga.tumor.hugo.tsv" {
		t.Fatalf("expected relative hierarchy, got %q", got)
	}
}

func TestUploadRecordPathMakesAbsolutePathRelativeToCWD(t *testing.T) {
	tmpDir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	target := filepath.Join(tmpDir, "data", "nested", "file.tsv")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatalf("mkdirall failed: %v", err)
	}
	if err := os.WriteFile(target, []byte("payload"), 0o644); err != nil {
		t.Fatalf("writefile failed: %v", err)
	}
	got, err := uploadRecordPath(target)
	if err != nil {
		t.Fatalf("uploadRecordPath returned error: %v", err)
	}
	if got != "data/nested/file.tsv" {
		t.Fatalf("expected cwd-relative path, got %q", got)
	}
}

func TestUploadRecordPathFallsBackToBaseNameOutsideCWD(t *testing.T) {
	tmpDir := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}
	defer func() {
		_ = os.Chdir(oldWD)
	}()

	outside := filepath.Join(string(filepath.Separator), "tmp", "outside", "file.tsv")
	got, err := uploadRecordPath(outside)
	if err != nil {
		t.Fatalf("uploadRecordPath returned error: %v", err)
	}
	if got != "file.tsv" {
		t.Fatalf("expected basename fallback, got %q", got)
	}
}

func TestHashFileSHA256HandlesLargeFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create test file: %v", err)
	}

	block := []byte("syfon upload checksum streaming test\n")
	const size = 16 << 20
	expected := sha256.New()
	for written := 0; written < size; {
		chunk := block
		if remaining := size - written; remaining < len(chunk) {
			chunk = chunk[:remaining]
		}
		n, err := file.Write(chunk)
		if err != nil {
			_ = file.Close()
			t.Fatalf("write test file: %v", err)
		}
		if n != len(chunk) {
			_ = file.Close()
			t.Fatalf("short test file write: wrote %d of %d bytes", n, len(chunk))
		}
		_, _ = expected.Write(chunk)
		written += n
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close test file: %v", err)
	}

	got, err := hashFileSHA256(path)
	if err != nil {
		t.Fatalf("hashFileSHA256 returned error: %v", err)
	}
	if want := hex.EncodeToString(expected.Sum(nil)); got != want {
		t.Fatalf("hashFileSHA256 = %s, want %s", got, want)
	}
	if gotSize, err := os.Stat(path); err != nil {
		t.Fatalf("stat test file: %v", err)
	} else if gotSize.Size() != size {
		t.Fatalf("test file size = %d, want %d", gotSize.Size(), size)
	}
}
