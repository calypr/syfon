package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/storage/address"
	"github.com/calypr/syfon/internal/testutils"
)

type probePortFake struct {
	calls   int
	results []storage.ProbeResult
}

func (f *probePortFake) Probe(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
	f.calls++
	if f.results != nil {
		return f.results
	}
	results := make([]storage.ProbeResult, len(targets))
	for i, target := range targets {
		results[i] = storage.ProbeResult{ID: target.ID, Target: target.Target, Metadata: storage.ObjectMetadata{
			Provider: address.S3Provider,
			Bucket:   target.Target.Bucket,
			Key:      target.Target.Key,
			Path:     target.Target.Key,
			ETag:     "etag",
		}}
	}
	return results
}

func TestCoreProbeMapsStorageOperationErrorsByProvider(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{Credentials: map[string]buckets.Credential{
		"bucket": {Bucket: "bucket", Provider: address.S3Provider},
	}}}
	for _, tc := range []struct {
		name     string
		provider string
		kind     storage.ErrorKind
		want     StorageInspectErrorKind
	}{
		{name: "credential missing", kind: storage.ErrorNotFound, want: StorageInspectCredentialMissing},
		{name: "object missing", provider: address.S3Provider, kind: storage.ErrorNotFound, want: StorageInspectObjectNotFound},
		{name: "unavailable", provider: address.S3Provider, kind: storage.ErrorUnavailable, want: StorageInspectBucketUnavailable},
		{name: "unsupported", provider: address.S3Provider, kind: storage.ErrorUnsupported, want: StorageInspectUnsupported},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &probePortFake{results: []storage.ProbeResult{{Err: &storage.OperationError{
				Kind:       tc.kind,
				Provider:   tc.provider,
				Capability: "probe",
			}}}}
			om := newTestObjectManager(db, StoragePorts{Probe: probe})
			_, err := om.InspectStorageObject(context.Background(), InspectStorageRequest{ObjectURL: "s3://bucket/key"})
			if err == nil {
				t.Fatal("expected probe error")
			}
			var inspectErr *StorageInspectError
			if !errors.As(err, &inspectErr) || inspectErr.Kind != tc.want {
				t.Fatalf("expected %s, got %T %v", tc.want, err, err)
			}
		})
	}
}

func TestCoreInspectionKeepsPolicyGatesBeforeProbe(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{Credentials: map[string]buckets.Credential{
		"bucket": {Bucket: "bucket", Provider: address.S3Provider},
	}}}
	probe := &probePortFake{}
	om := newTestObjectManager(db, StoragePorts{Probe: probe})
	_, err := om.InspectStorageObject(context.Background(), InspectStorageRequest{
		Organization: "org",
		Project:      "project",
		Key:          "key",
		Scheme:       address.S3Provider,
	})
	if err == nil || !strings.Contains(err.Error(), "no bucket scope configured") {
		t.Fatalf("expected scope policy error, got %v", err)
	}
	if probe.calls != 0 {
		t.Fatalf("probe ran before scope policy gate: %d calls", probe.calls)
	}
}

type inventoryPortFake struct {
	result storage.InventoryResult
	err    error
	calls  []storage.InventoryRequest
}

func (f *inventoryPortFake) Inventory(_ context.Context, request storage.InventoryRequest) (storage.InventoryResult, error) {
	f.calls = append(f.calls, request)
	return f.result, f.err
}

func TestCoreInventoryMapsPartialStorageResult(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	inventory := &inventoryPortFake{result: storage.InventoryResult{
		Items:    []storage.ObjectMetadata{{Provider: address.S3Provider, Bucket: "bucket", Key: "prefix/object.txt", SizeBytes: 7}},
		Complete: false,
	}}
	om := newTestObjectManager(db, StoragePorts{Inventory: inventory})
	result, err := om.InspectProjectStorage(context.Background(), "org", "project", ProjectStorageInspectOptions{Mode: ProjectStorageInspectItems})
	if err != nil {
		t.Fatalf("InspectProjectStorage returned error: %v", err)
	}
	if result.Summary.InventoryComplete || result.Summary.InventoryWarning == "" || len(result.Items) != 1 {
		t.Fatalf("expected partial items and warning, got %+v", result)
	}
	if len(inventory.calls) != 1 || inventory.calls[0].Target.Prefix != "prefix" {
		t.Fatalf("unexpected inventory request: %+v", inventory.calls)
	}
}

type deletePortFake struct {
	calls [][]storage.DeleteTarget
}

func (f *deletePortFake) DeleteExact(_ context.Context, targets []storage.DeleteTarget) error {
	f.calls = append(f.calls, append([]storage.DeleteTarget(nil), targets...))
	return nil
}

func TestCoreProjectDeletePreservesPolicyOrderAndPerItemDispatch(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	deleter := &deletePortFake{}
	om := newTestObjectManager(db, StoragePorts{Delete: deleter})
	results := om.DeleteProjectStorageObjects(context.Background(), "org", "project", []string{
		"s3://bucket/prefix/a.txt",
		"s3://bucket/outside.txt",
		"s3://bucket/prefix/a.txt",
	})
	if len(results) != 2 || results[0].Status != "deleted" || results[1].Status != "forbidden" {
		t.Fatalf("unexpected project delete results: %+v", results)
	}
	if len(deleter.calls) != 1 || len(deleter.calls[0]) != 1 || deleter.calls[0][0].Location != "s3://bucket/prefix/a.txt" {
		t.Fatalf("unexpected delete calls: %+v", deleter.calls)
	}
}

func TestCoreStorageDeleteConflictHasNoProviderSideEffect(t *testing.T) {
	db := &testutils.MockDatabase{Objects: map[string]*objects.Record{"object": {Id: "object"}}}
	deleter := &deletePortFake{}
	om := newTestObjectManager(db, StoragePorts{Delete: deleter})
	err := om.DeleteObjectWithOptions(context.Background(), "object", DeleteOptions{DeleteStorageData: true})
	if !errors.Is(err, faults.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if len(deleter.calls) != 0 {
		t.Fatalf("provider delete ran on conflict: %+v", deleter.calls)
	}
}
