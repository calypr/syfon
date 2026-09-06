package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
	"github.com/calypr/syfon/internal/testutils"
)

func TestStorageCompositionSharesOneManagerAcrossCorePorts(t *testing.T) {
	root := t.TempDir()
	credentials := &testutils.MockDatabase{Credentials: map[string]buckets.Credential{
		"bucket": {Bucket: "bucket", Provider: address.FileProvider, Endpoint: root},
	}}
	manager, err := newStorageManager(credentials, root, nil)
	if err != nil {
		t.Fatalf("newStorageManager: %v", err)
	}
	ports := storagePorts(manager)
	if ports.Access != manager || ports.Multipart != manager || ports.Probe != manager || ports.Inventory != manager || ports.Delete != manager {
		t.Fatalf("storage ports do not share one manager: %+v", ports)
	}

	access, err := ports.Access.Access(context.Background(), storage.AccessRequest{
		Target: storage.AccessTarget{Location: "s3://bucket/object"},
	})
	if err != nil {
		t.Fatalf("file-backed access through composed manager: %v", err)
	}
	if want := filepath.Join(root, "object"); access.Location != filepath.ToSlash(want) {
		t.Fatalf("access location=%q, want %q", access.Location, filepath.ToSlash(want))
	}

	invalidator := &storageInvalidator{}
	invalidator.InvalidateBucket("bucket")
	invalidator.manager = manager
	invalidator.InvalidateBucket("bucket")
}
