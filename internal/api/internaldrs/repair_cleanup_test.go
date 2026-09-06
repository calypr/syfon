package internaldrs

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/repair"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/testutils"
)

func TestStorageRepairInspectorReturnsCanonicalURL(t *testing.T) {
	db := storageRepairTestDatabase()
	probeFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
		return []storage.ProbeResult{{Target: targets[0].Target, Metadata: storage.ObjectMetadata{Provider: "s3", Bucket: targets[0].Target.Bucket, Key: targets[0].Target.Key}}}
	}}
	om := newInternalDRSObjectManager(db, core.StoragePorts{Probe: probeFake})

	got, err := (storageRepairInspector{om: om.ObjectManager}).Inspect(context.Background(), repair.StorageInspectRequest{
		Organization: "org",
		Project:      "proj",
		ObjectURL:    " s3://b1/prefix/object.bin ",
	})
	if err != nil {
		t.Fatalf("inspect failed: %v", err)
	}
	if got.ObjectURL != "s3://b1/prefix/object.bin" {
		t.Fatalf("unexpected canonical object URL: %q", got.ObjectURL)
	}
}

func TestStorageRepairInspectorClassifiesMissingObject(t *testing.T) {
	probeFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
		return []storage.ProbeResult{{Target: targets[0].Target, Err: &storage.OperationError{Kind: storage.ErrorNotFound, Provider: "s3", Capability: "probe"}}}
	}}
	om := newInternalDRSObjectManager(storageRepairTestDatabase(), core.StoragePorts{Probe: probeFake})

	_, err := (storageRepairInspector{om: om.ObjectManager}).Inspect(context.Background(), repair.StorageInspectRequest{ObjectURL: "s3://b1/prefix/object.bin"})
	if !errors.Is(err, repair.ErrStorageObjectNotFound) {
		t.Fatalf("expected repair missing-object error, got %v", err)
	}
}

func TestStorageRepairInspectorPreservesAuthorizationAndCredentialFailures(t *testing.T) {
	t.Run("authorization", func(t *testing.T) {
		probeFake := &internalDRSProbeFake{probeFn: func(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
			return []storage.ProbeResult{{Target: targets[0].Target, Err: &storage.OperationError{Kind: storage.ErrorForbidden, Provider: "s3", Capability: "probe"}}}
		}}
		om := newInternalDRSObjectManager(storageRepairTestDatabase(), core.StoragePorts{Probe: probeFake})
		_, err := (storageRepairInspector{om: om.ObjectManager}).Inspect(policyTestContext("gen3", true, nil), repair.StorageInspectRequest{ObjectURL: "s3://b1/prefix/object.bin"})
		var inspectErr *core.StorageInspectError
		if !errors.As(err, &inspectErr) || inspectErr.Kind != core.StorageInspectPermissionDenied {
			t.Fatalf("expected permission-denied inspection error, got %v", err)
		}
	})

	t.Run("credential", func(t *testing.T) {
		db := &testutils.MockDatabase{NoDefaultCreds: true}
		om := newInternalDRSObjectManager(db, &internalDRSStorageFake{})
		_, err := (storageRepairInspector{om: om.ObjectManager}).Inspect(context.Background(), repair.StorageInspectRequest{ObjectURL: "s3://missing/prefix/object.bin"})
		var inspectErr *core.StorageInspectError
		if !errors.As(err, &inspectErr) || inspectErr.Kind != core.StorageInspectCredentialMissing {
			t.Fatalf("expected credential-missing inspection error, got %v", err)
		}
	})
}

func TestStorageRepairInspectorRejectsMalformedTarget(t *testing.T) {
	om := newInternalDRSObjectManager(storageRepairTestDatabase(), &internalDRSStorageFake{})
	_, err := (storageRepairInspector{om: om.ObjectManager}).Inspect(context.Background(), repair.StorageInspectRequest{ObjectURL: "https://example.com/object.bin"})
	var inspectErr *core.StorageInspectError
	if !errors.As(err, &inspectErr) || inspectErr.Kind != core.StorageInspectInvalidInput {
		t.Fatalf("expected invalid-target inspection error, got %v", err)
	}
}

func storageRepairTestDatabase() *testutils.MockDatabase {
	return &testutils.MockDatabase{
		Credentials: map[string]buckets.Credential{
			"b1": {CredentialID: "b1", Bucket: "b1", Provider: "s3"},
		},
		BucketScopes: map[string]buckets.Scope{
			"org|proj": {Organization: "org", ProjectID: "proj", Bucket: "b1"},
		},
	}
}

func TestScopeRepairApplyRejectsReadOnlyProjectUser(t *testing.T) {
	resource, err := sycommon.ResourcePath("org", "proj")
	if err != nil {
		t.Fatalf("resource path: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/data/repair/project-scope/apply", strings.NewReader(`{"organization":"org","project":"proj"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(policyTestContext("gen3", true, map[string]map[string]bool{
		resource: {"read": true},
	}))

	rr := doInternalDRSTestRequest(req, newInternalDRSObjectManager(&testutils.MockDatabase{}, &internalDRSStorageFake{}))
	if rr.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only caller, got %d body=%s", rr.Code, rr.Body.String())
	}
}
