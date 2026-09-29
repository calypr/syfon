package transfers

import (
	"context"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/client/services"
)

func TestCanonicalUploadKeyReachesResolvedStorageTarget(t *testing.T) {
	const objectKey = "dir/slash?query#fragment%literal%2F"
	signedURL := "https://upload.example/bucket/dir%2Fslash%3Fquery%23fragment%25literal%252F?signature=secret"

	canonicalURL, err := (&services.DataService{}).CanonicalObjectURL(signedURL, "bucket", "")
	if err != nil {
		t.Fatalf("CanonicalObjectURL() error = %v", err)
	}
	resolved, err := NewService(Dependencies{}).ResolveCanonicalStorageTarget(context.Background(), CanonicalStorageTargetRequest{
		Object:    &drs.DrsObject{Id: "object"},
		AccessURL: canonicalURL,
	})
	if err != nil {
		t.Fatalf("ResolveCanonicalStorageTarget() error = %v", err)
	}
	if resolved.Bucket != "bucket" || resolved.Key != objectKey {
		t.Fatalf("resolved target = %+v, want bucket=%q key=%q", resolved, "bucket", objectKey)
	}

	signingTarget := storageTargetFromCanonical(canonicalURL, resolved)
	if signingTarget.PhysicalBucket != "bucket" || signingTarget.Key != objectKey {
		t.Fatalf("storage signing target = %+v, want bucket=%q key=%q", signingTarget, "bucket", objectKey)
	}
}
