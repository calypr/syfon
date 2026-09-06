package core

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/calypr/syfon/internal/access"
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

func TestCoreProbeConvertsMetadataAndValidatesExpectations(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	probe := &probePortFake{results: []storage.ProbeResult{{Metadata: storage.ObjectMetadata{
		Provider:   address.S3Provider,
		Bucket:     "bucket",
		Key:        "prefix/object.txt",
		Path:       "object.txt",
		SizeBytes:  42,
		MetaSHA256: "abcdef",
		ETag:       "etag",
	}}}}
	om := newTestObjectManager(db, StoragePorts{Probe: probe})
	expectedSize := int64(42)
	results := om.InspectStorageObjects(context.Background(), []InspectStorageRequest{{
		ID:                "item-1",
		Organization:      "org",
		Project:           "project",
		Key:               "object.txt",
		Scheme:            address.S3Provider,
		ExpectedSizeBytes: &expectedSize,
		ExpectedSHA256:    "sha256:abcdef",
	}})
	if len(results) != 1 {
		t.Fatalf("expected one result, got %+v", results)
	}
	result := results[0]
	if result.Status != StorageProbeStatusPresent || result.Provider != address.S3Provider || result.Bucket != "bucket" || result.Key != "prefix/object.txt" || result.Path != "object.txt" {
		t.Fatalf("unexpected converted metadata: %+v", result)
	}
	if result.SizeBytes == nil || *result.SizeBytes != 42 || result.MetaSHA256 != "abcdef" || result.ETag != "etag" {
		t.Fatalf("unexpected metadata values: %+v", result)
	}
	if result.ValidationStatus != StorageValidationMatched || result.SizeMatch == nil || !*result.SizeMatch || result.SHA256Match == nil || !*result.SHA256Match {
		t.Fatalf("expected matched validation, got %+v", result)
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

func TestCoreRawInspectionAuthzFailureSkipsProbe(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	probe := &probePortFake{}
	om := newTestObjectManager(db, StoragePorts{Probe: probe})
	session := access.NewSession("test")
	session.AuthzEnforced = true
	session.SetAuthorizations(nil, map[string]map[string]bool{}, true)
	_, err := om.InspectStorageObject(access.WithSession(context.Background(), session), InspectStorageRequest{
		Organization: "org",
		Project:      "project",
		Key:          "object.txt",
		Scheme:       address.S3Provider,
	})
	if err == nil || !errors.Is(err, faults.ErrUnauthorized) {
		t.Fatalf("expected authorization error, got %v", err)
	}
	if probe.calls != 0 {
		t.Fatalf("probe ran before authorization gate: %d calls", probe.calls)
	}
}

type rawVisibilityErrorDB struct {
	*coreTestDB
	err error
}

func (d *rawVisibilityErrorDB) ListBucketVisibilityRows(context.Context, []string, bool, bool) ([]buckets.VisibilityRow, error) {
	return nil, d.err
}

func TestCoreRawInspectionVisibilityFailureSkipsProbe(t *testing.T) {
	wantErr := errors.New("visibility lookup failed")
	db := &rawVisibilityErrorDB{coreTestDB: &coreTestDB{MockDatabase: &testutils.MockDatabase{Credentials: map[string]buckets.Credential{
		"bucket": {Bucket: "bucket", Provider: address.S3Provider},
	}}}, err: wantErr}
	probe := &probePortFake{}
	om := newTestObjectManager(db, StoragePorts{Probe: probe})
	_, err := om.InspectStorageObject(context.Background(), InspectStorageRequest{ObjectURL: "s3://bucket/key"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected visibility error, got %v", err)
	}
	if probe.calls != 0 {
		t.Fatalf("probe ran before visibility gate: %d calls", probe.calls)
	}
}

func TestCoreBulkProbeEmptyResultIsNonNil(t *testing.T) {
	om := newTestObjectManager(&testutils.MockDatabase{}, StoragePorts{})
	results := om.InspectStorageObjects(context.Background(), nil)
	if results == nil || len(results) != 0 {
		t.Fatalf("expected non-nil empty bulk result, got %#v", results)
	}
}

func TestCoreProbeProviderErrorPreservesGenericStatus(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{Credentials: map[string]buckets.Credential{
		"bucket": {Bucket: "bucket", Provider: address.S3Provider},
	}}}
	probe := &probePortFake{results: []storage.ProbeResult{{Err: &storage.OperationError{
		Kind:       storage.ErrorProvider,
		Provider:   address.S3Provider,
		Capability: "probe",
		Cause:      errors.New("provider exploded"),
	}}}}
	om := newTestObjectManager(db, StoragePorts{Probe: probe})
	results := om.InspectStorageObjects(context.Background(), []InspectStorageRequest{{ID: "item", ObjectURL: "s3://bucket/key"}})
	if len(results) != 1 || results[0].Status != StorageProbeStatusError || results[0].ErrorKind != "error" || !strings.Contains(results[0].Error, "provider exploded") {
		t.Fatalf("expected generic provider error result, got %+v", results)
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

func TestCoreInventoryMapsIncompleteOperationWithPartialItems(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	inventory := &inventoryPortFake{
		result: storage.InventoryResult{Items: []storage.ObjectMetadata{{Provider: address.S3Provider, Bucket: "bucket", Key: "prefix/object.txt"}}, Complete: false},
		err:    &storage.OperationError{Kind: storage.ErrorIncomplete, Provider: address.S3Provider, Capability: "inventory", Cause: errors.New("page 2 failed")},
	}
	om := newTestObjectManager(db, StoragePorts{Inventory: inventory})
	result, err := om.InspectProjectStorage(context.Background(), "org", "project", ProjectStorageInspectOptions{Mode: ProjectStorageInspectItems})
	if err != nil {
		t.Fatalf("InspectProjectStorage returned error for partial inventory: %v", err)
	}
	if result.Summary.InventoryComplete || !strings.Contains(result.Summary.InventoryWarning, "page 2 failed") || len(result.Items) != 1 {
		t.Fatalf("expected incomplete warning and partial item, got %+v", result)
	}
}

type deletePortFake struct {
	calls    [][]storage.DeleteTarget
	errByURL map[string]error
}

func (f *deletePortFake) DeleteExact(_ context.Context, targets []storage.DeleteTarget) error {
	f.calls = append(f.calls, append([]storage.DeleteTarget(nil), targets...))
	if len(targets) == 1 && f.errByURL != nil {
		return f.errByURL[targets[0].Location]
	}
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

func TestCoreProjectDeleteMixedResultsPreserveOrder(t *testing.T) {
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"bucket": {Bucket: "bucket", Provider: address.S3Provider},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket", PathPrefix: "prefix"},
		},
	}}
	providerErr := &storage.OperationError{Kind: storage.ErrorProvider, Provider: address.S3Provider, Capability: "delete", Cause: errors.New("delete failed")}
	deleter := &deletePortFake{errByURL: map[string]error{"s3://bucket/prefix/error.txt": providerErr}}
	om := newTestObjectManager(db, StoragePorts{Delete: deleter})
	results := om.DeleteProjectStorageObjects(context.Background(), "org", "project", []string{
		"s3://bucket/prefix/good.txt",
		"https://example.invalid/not-storage",
		"s3://bucket/outside.txt",
		"s3://bucket/prefix/error.txt",
	})
	if len(results) != 4 {
		t.Fatalf("expected four results, got %+v", results)
	}
	want := []string{"deleted", "invalid", "forbidden", "error"}
	for i, status := range want {
		if results[i].Status != status {
			t.Fatalf("result %d status=%q, want %q: %+v", i, results[i].Status, status, results)
		}
	}
	if len(deleter.calls) != 2 || deleter.calls[0][0].Location != "s3://bucket/prefix/good.txt" || deleter.calls[1][0].Location != "s3://bucket/prefix/error.txt" {
		t.Fatalf("unexpected accepted-target calls: %+v", deleter.calls)
	}
}

func TestCoreScopedStorageHelperContracts(t *testing.T) {
	scopes := []buckets.Scope{{PathPrefix: "org"}, {PathPrefix: "project"}}
	if got := normalizeScopedStorageKey("org/project/object.txt", scopes); got != "org/project/object.txt" {
		t.Fatalf("expected already-prefixed key to remain stable, got %q", got)
	}
	if got := normalizeScopedStorageKey("", scopes); got != "org/project" {
		t.Fatalf("expected joined scope prefix, got %q", got)
	}
	if !projectStorageKeyWithinPrefix("org/project/object.txt", "org/project") || projectStorageKeyWithinPrefix("org/other/object.txt", "org/project") {
		t.Fatal("unexpected project prefix containment result")
	}
	if got := storageListDirectoryPrefix("org/project/object.txt"); got != "org/project/" {
		t.Fatalf("unexpected directory prefix %q", got)
	}
}

func TestCoreDeletePreservesCredentialProviderOverrideAndLookupOrder(t *testing.T) {
	root := t.TempDir()
	db := &coreTestDB{MockDatabase: &testutils.MockDatabase{NoDefaultCreds: true, Credentials: map[string]buckets.Credential{
		"logical": {Bucket: "logical", Provider: address.FileProvider, Endpoint: root},
	}}}
	om := newTestObjectManager(db, StoragePorts{})
	target, ok, err := om.storageTargetFromURL(context.Background(), "s3://logical/path/to/object")
	if err != nil || !ok {
		t.Fatalf("storage target resolution failed: ok=%v err=%v", ok, err)
	}
	if target.provider != address.FileProvider || target.path != filepath.Join(root, "path", "to", "object") || target.location != "s3://logical/path/to/object" {
		t.Fatalf("credential provider override changed: %+v", target)
	}
	if _, ok, err := om.storageTargetFromURL(context.Background(), "s3://missing/key"); err == nil || ok || !strings.Contains(err.Error(), "lookup credential for bucket missing") {
		t.Fatalf("expected missing-credential prelookup error before dispatch, ok=%v err=%v", ok, err)
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
