package upload

import (
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/client/bucketapi"
)

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
