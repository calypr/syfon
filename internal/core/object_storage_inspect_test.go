package core

import "testing"

func TestClassifyProbeErrorCredentialMissingIsNotBucketMiss(t *testing.T) {
	status, kind := classifyStorageProbeError(&StorageInspectError{
		Kind:    StorageInspectCredentialMissing,
		Message: `no stored bucket credential found for bucket "missing"`,
	})
	if status != StorageProbeStatusInvalid || kind != "credential_missing" {
		t.Fatalf("expected credential_missing to classify as invalid status, got status=%q kind=%q", status, kind)
	}
}

func TestClassifyProbeErrorObjectNotFoundRemainsBucketMiss(t *testing.T) {
	status, kind := classifyStorageProbeError(&StorageInspectError{
		Kind:    StorageInspectObjectNotFound,
		Message: "provider could not find s3://bucket/key",
	})
	if status != StorageProbeStatusNotFound || kind != "object_not_found" {
		t.Fatalf("expected object_not_found to classify as not_found, got status=%q kind=%q", status, kind)
	}
}
