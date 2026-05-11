package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/testutils"
)

func TestStorageTargetFromURLVariants(t *testing.T) {
	t.Run("file backed bucket resolves to local filesystem path", func(t *testing.T) {
		root := t.TempDir()
		om := NewObjectManager(&testutils.MockDatabase{
			Credentials: map[string]models.S3Credential{
				"bucket": {Bucket: "bucket", Provider: common.FileProvider, Endpoint: root},
			},
		}, &capturingURLManager{})

		target, ok, err := om.storageTargetFromURL(context.Background(), "s3://bucket/a/b.txt")
		if err != nil {
			t.Fatalf("storageTargetFromURL failed: %v", err)
		}
		if !ok {
			t.Fatal("expected target to resolve")
		}
		if target.provider != common.FileProvider {
			t.Fatalf("unexpected provider: %+v", target)
		}
		if want := filepath.Join(root, "a", "b.txt"); target.path != want {
			t.Fatalf("unexpected local path: got %q want %q", target.path, want)
		}
	})

	t.Run("absolute local path is treated as file target", func(t *testing.T) {
		om := NewObjectManager(&testutils.MockDatabase{}, &capturingURLManager{})
		target, ok, err := om.storageTargetFromURL(context.Background(), "/tmp/example.txt")
		if err != nil {
			t.Fatalf("storageTargetFromURL failed: %v", err)
		}
		if !ok || target.provider != common.FileProvider || target.path != "/tmp/example.txt" {
			t.Fatalf("unexpected target: %+v ok=%v", target, ok)
		}
	})

	t.Run("unsupported and incomplete urls are ignored", func(t *testing.T) {
		om := NewObjectManager(&testutils.MockDatabase{}, &capturingURLManager{})
		if _, ok, err := om.storageTargetFromURL(context.Background(), "https://example.org/object"); err != nil || ok {
			t.Fatalf("expected https url to be ignored, got ok=%v err=%v", ok, err)
		}
		if _, ok, err := om.storageTargetFromURL(context.Background(), "s3://bucket"); err != nil || ok {
			t.Fatalf("expected empty-key s3 url to be ignored, got ok=%v err=%v", ok, err)
		}
	})
}

func TestDeleteStorageTargetFileProvider(t *testing.T) {
	root := t.TempDir()
	targetPath := filepath.Join(root, "nested", "file.txt")
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(targetPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	om := NewObjectManager(&testutils.MockDatabase{}, &capturingURLManager{})
	if err := om.deleteStorageTarget(context.Background(), storageTarget{provider: common.FileProvider, path: targetPath}); err != nil {
		t.Fatalf("deleteStorageTarget(existing) failed: %v", err)
	}
	if _, err := os.Stat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("expected file to be removed, stat err=%v", err)
	}
	if err := om.deleteStorageTarget(context.Background(), storageTarget{provider: common.FileProvider, path: targetPath}); err != nil {
		t.Fatalf("deleteStorageTarget(missing) failed: %v", err)
	}
}

func TestStorageTargetsForScopedObjectUseCanonicalChecksumPath(t *testing.T) {
	checksum := strings.Repeat("a", 64)
	om := NewObjectManager(&testutils.MockDatabase{
		Credentials: map[string]models.S3Credential{
			"syfon-e2e-bucket": {Bucket: "syfon-e2e-bucket", Provider: "s3", Region: "us-west-2"},
		},
		BucketScopes: map[string]models.BucketScope{
			"syfon|": {
				Organization: "syfon",
				Bucket:       "syfon-e2e-bucket",
				PathPrefix:   "program-root",
			},
			"syfon|e2e": {
				Organization: "syfon",
				ProjectID:    "e2e",
				Bucket:       "syfon-e2e-bucket",
				PathPrefix:   "project-subpath",
			},
		},
	}, &capturingURLManager{})

	obj := &models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:               "f781273b-52eb-5ac2-a484-775235eef303",
			ControlledAccess: &[]string{"/organization/syfon/project/e2e"},
			Checksums:        []drs.Checksum{{Type: "sha256", Checksum: checksum}},
			AccessMethods: &[]drs.AccessMethod{{
				Type: drs.AccessMethodTypeS3,
				AccessUrl: &struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: "s3://objects/f781273b-52eb-5ac2-a484-775235eef303"},
			}},
		},
	}

	targets, err := om.storageTargetsForObject(context.Background(), obj)
	if err != nil {
		t.Fatalf("storageTargetsForObject failed: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("expected one canonical target, got %+v", targets)
	}
	if targets[0].bucket != "syfon-e2e-bucket" {
		t.Fatalf("expected scoped bucket syfon-e2e-bucket, got %q", targets[0].bucket)
	}
	if want := "program-root/project-subpath/" + checksum; targets[0].key != want {
		t.Fatalf("expected canonical delete key %q, got %q", want, targets[0].key)
	}
}

func TestScopedStorageHelperUtilities(t *testing.T) {
	bucket, key, ok := parseS3Location("s3://bucket-name/path/to/object")
	if !ok || bucket != "bucket-name" || key != "path/to/object" {
		t.Fatalf("unexpected parsed s3 location: bucket=%q key=%q ok=%v", bucket, key, ok)
	}

	if got := normalizeScopedStorageKey("org/project/object.txt", []models.BucketScope{
		{PathPrefix: "org"},
		{PathPrefix: "project"},
	}); got != "org/project/object.txt" {
		t.Fatalf("expected already-prefixed key to remain stable, got %q", got)
	}

	if got := normalizeScopedStorageKey("", []models.BucketScope{
		{PathPrefix: "org"},
		{PathPrefix: "project"},
	}); got != "org/project" {
		t.Fatalf("unexpected empty-key normalization: %q", got)
	}

	if got := trimLeadingStoragePrefix("org/project/object.txt", "org"); got != "project/object.txt" {
		t.Fatalf("unexpected trimmed key: %q", got)
	}

	if got := azureServiceURL("acct", "127.0.0.1:10000/devstoreaccount1"); got != "https://127.0.0.1:10000/devstoreaccount1" {
		t.Fatalf("unexpected azure service url: %q", got)
	}
	if got := azureAccountFromEndpoint("http://devstoreaccount1.blob.localhost:10000"); got != "devstoreaccount1" {
		t.Fatalf("unexpected azure account name: %q", got)
	}
}
