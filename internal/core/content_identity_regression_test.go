package core

import (
	"context"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
)

type staticContentReader struct {
	objects []objects.Record
}

func (r staticContentReader) GetObjectsByChecksum(context.Context, string) ([]objects.Record, error) {
	return r.objects, nil
}

func (r staticContentReader) GetObjectsByChecksums(_ context.Context, checksums []string) (map[string][]objects.Record, error) {
	out := make(map[string][]objects.Record, len(checksums))
	for _, checksum := range checksums {
		out[checksum] = r.objects
	}
	return out, nil
}

func TestMergedContentPreservesReplicaLocation(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{BucketScopes: map[string]buckets.Scope{
		"org|a": {Organization: "org", ProjectID: "a", Bucket: "bucket-a", PathPrefix: "a"},
		"org|b": {Organization: "org", ProjectID: "b", Bucket: "bucket-b", PathPrefix: "b"},
	}}}
	om := newTestObjectManager(db, &capturingURLManager{})
	objs := []objects.Record{
		{Id: "uuid-a", Checksums: []objects.Checksum{{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, ControlledAccess: &[]string{"/organization/org/project/a"}},
		{Id: "uuid-b", Checksums: []objects.Checksum{{Type: "sha256", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}, ControlledAccess: &[]string{"/organization/org/project/b"}},
	}
	original := "s3://bucket-a/a/file"
	single, err := om.SignObjectURL(context.Background(), &objs[0], original, storage.AccessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if single != "signed:"+original {
		t.Fatalf("single project changed replica: %s", single)
	}
	service := objects.NewService(objects.Dependencies{Content: staticContentReader{objects: objs}})
	view, err := service.GetCanonicalContent(context.Background(), objs[0].Checksums[0].Checksum, "")
	if err != nil {
		t.Fatal(err)
	}
	merged := view.Record
	signed, err := om.SignObjectURL(context.Background(), &merged, original, storage.AccessOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if signed != "signed:"+original {
		t.Fatalf("merged read changed replica: got %s, want signed:%s", signed, original)
	}
}
