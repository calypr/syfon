package drs

import (
	"strings"
	"testing"
)

func TestObjectBuilderBuildSuccess(t *testing.T) {
	builder := ObjectBuilder{
		ProjectID: "test-project",
		Bucket:    "bucket",
	}

	obj, err := builder.Build("file.txt", "sha-256", 12, "did-1")
	if err != nil {
		t.Fatalf("Build error: %v", err)
	}
	if obj.Id != "did-1" {
		t.Fatalf("unexpected Id: %s", obj.Id)
	}
	if obj.Name == nil || *obj.Name != "file.txt" {
		t.Fatalf("unexpected Name: %+v", obj.Name)
	}
	if obj.Checksums[0].Checksum != "sha-256" {
		t.Fatalf("unexpected checksum: %v", obj.Checksums)
	}
	if obj.Size != 12 {
		t.Fatalf("unexpected size: %d", obj.Size)
	}
	if obj.AccessMethods == nil || len(*obj.AccessMethods) != 1 {
		t.Fatalf("expected 1 access method, got %+v", obj.AccessMethods)
	}
	if (*obj.AccessMethods)[0].AccessUrl == nil || !strings.Contains((*obj.AccessMethods)[0].AccessUrl.Url, "bucket/test/project/sha-256") {
		t.Fatalf("unexpected access URL: %+v", (*obj.AccessMethods)[0].AccessUrl)
	}
	if obj.Aliases != nil && len(*obj.Aliases) != 0 {
		t.Fatalf("expected no aliases, got: %#v", obj.Aliases)
	}
	if (*obj.AccessMethods)[0].Type != "s3" {
		t.Fatalf("unexpected access method type: %s", (*obj.AccessMethods)[0].Type)
	}
}

func TestObjectBuilderBuildEmptyBucket(t *testing.T) {
	builder := ObjectBuilder{
		ProjectID: "test-project",
		Bucket:    "",
	}

	if _, err := builder.Build("file.txt", "sha-256", 12, "did-1"); err == nil {
		t.Fatalf("expected error when Bucket is empty")
	}
}
