package internaldrs

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/maintenance/scoperepair"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/gofiber/fiber/v3"
)

type repairObjectServiceFake struct {
	record  objects.Record
	queries []struct {
		organization string
		project      string
		method       string
		start        string
		limit        int
		offset       int
	}
	getMethod    string
	replacements []objects.Record
	collapse     []string
}

func (f *repairObjectServiceFake) ListPreparedObjectsPageByScope(_ context.Context, organization, project, method, start string, limit, offset int) ([]objects.Record, error) {
	f.queries = append(f.queries, struct {
		organization string
		project      string
		method       string
		start        string
		limit        int
		offset       int
	}{organization, project, method, start, limit, offset})
	if f.record.Id == "" {
		return nil, nil
	}
	return []objects.Record{f.record}, nil
}

func (f *repairObjectServiceFake) GetObject(_ context.Context, _ string, method string) (*objects.Record, error) {
	f.getMethod = method
	record := f.record
	return &record, nil
}

func (f *repairObjectServiceFake) ReplaceObjects(_ context.Context, records []objects.Record) error {
	f.replacements = append(f.replacements, records...)
	return nil
}

func (f *repairObjectServiceFake) CollapseProjectChecksumDuplicates(_ context.Context, organization, project string) (int, error) {
	f.collapse = append(f.collapse, organization+"/"+project)
	return 0, nil
}

type repairBucketServiceFake struct {
	credentials []buckets.Credential
	scopes      []buckets.Scope
}

func (f repairBucketServiceFake) ListS3Credentials(context.Context) ([]buckets.Credential, error) {
	return f.credentials, nil
}

func (f repairBucketServiceFake) ListBucketScopes(context.Context) ([]buckets.Scope, error) {
	return f.scopes, nil
}

type repairProbeFake struct {
	results []storage.ProbeResult
	calls   []storage.ProbeTarget
}

func (f *repairProbeFake) Probe(_ context.Context, targets []storage.ProbeTarget) []storage.ProbeResult {
	f.calls = append(f.calls, targets...)
	return append([]storage.ProbeResult(nil), f.results...)
}

type repairBucketAccessFake struct {
	credential    *buckets.Credential
	credentialErr error
	visible       map[string]buckets.VisibleBucket
	visibleErr    error
}

func (f repairBucketAccessFake) GetS3Credential(context.Context, string) (*buckets.Credential, error) {
	return f.credential, f.credentialErr
}

func (f repairBucketAccessFake) ListVisibleBuckets(context.Context) (map[string]buckets.VisibleBucket, error) {
	return f.visible, f.visibleErr
}

func TestStorageRepairInspectorReturnsCanonicalURL(t *testing.T) {
	probe := &repairProbeFake{results: []storage.ProbeResult{{Metadata: storage.ObjectMetadata{Provider: "s3"}}}}
	got, err := (storageRepairInspector{probe: probe}).Inspect(context.Background(), scoperepair.StorageInspectRequest{
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
	if len(probe.calls) != 1 || probe.calls[0].Target != (storage.ObjectTarget{Bucket: "b1", Key: "prefix/object.bin"}) {
		t.Fatalf("probe calls = %+v", probe.calls)
	}
}

func TestStorageRepairInspectorClassifiesMissingObject(t *testing.T) {
	probe := &repairProbeFake{results: []storage.ProbeResult{{Err: &storage.OperationError{Kind: storage.ErrorNotFound, Provider: "s3", Capability: "probe"}}}}
	_, err := (storageRepairInspector{probe: probe}).Inspect(context.Background(), scoperepair.StorageInspectRequest{ObjectURL: "s3://b1/prefix/object.bin"})
	if !errors.Is(err, scoperepair.ErrStorageObjectNotFound) {
		t.Fatalf("expected maintenance missing-object error, got %v", err)
	}
}

func TestStorageRepairInspectorPreservesAuthorizationAndCredentialFailures(t *testing.T) {
	t.Run("authorization", func(t *testing.T) {
		probeErr := &storage.OperationError{Kind: storage.ErrorForbidden, Provider: "s3", Capability: "probe"}
		probe := &repairProbeFake{results: []storage.ProbeResult{{Err: probeErr}}}
		adapter := storageRepairInspector{
			probe: probe,
			buckets: repairBucketAccessFake{
				credential: &buckets.Credential{CredentialID: "b1", Bucket: "b1", Provider: "s3"},
				visible:    map[string]buckets.VisibleBucket{"b1": {Credential: buckets.Credential{CredentialID: "b1", Bucket: "b1", Provider: "s3"}}},
			},
		}
		_, err := adapter.Inspect(context.Background(), scoperepair.StorageInspectRequest{ObjectURL: "s3://b1/prefix/object.bin"})
		if !errors.Is(err, probeErr) {
			t.Fatalf("expected permission error, got %v", err)
		}
	})

	t.Run("credential", func(t *testing.T) {
		credentialErr := errors.New("credential lookup failed")
		adapter := storageRepairInspector{buckets: repairBucketAccessFake{credentialErr: credentialErr}, probe: &repairProbeFake{}}
		_, err := adapter.Inspect(context.Background(), scoperepair.StorageInspectRequest{ObjectURL: "s3://missing/prefix/object.bin"})
		if !errors.Is(err, credentialErr) {
			t.Fatalf("expected credential error, got %v", err)
		}
	})

	t.Run("unsupported-provider", func(t *testing.T) {
		adapter := storageRepairInspector{buckets: repairBucketAccessFake{
			credential: &buckets.Credential{CredentialID: "b1", Bucket: "b1", Provider: "gcs"},
		}, probe: &repairProbeFake{}}
		_, err := adapter.Inspect(context.Background(), scoperepair.StorageInspectRequest{ObjectURL: "s3://b1/prefix/object.bin"})
		if err == nil || !strings.Contains(err.Error(), "not supported") {
			t.Fatalf("expected unsupported-provider error, got %v", err)
		}
	})
}

func TestStorageRepairInspectorRejectsMalformedTarget(t *testing.T) {
	probe := &repairProbeFake{}
	_, err := (storageRepairInspector{probe: probe}).Inspect(context.Background(), scoperepair.StorageInspectRequest{ObjectURL: "https://example.com/object.bin"})
	if err == nil || !strings.Contains(err.Error(), "valid s3://bucket/key") {
		t.Fatalf("expected invalid-target inspection error, got %v", err)
	}
	if len(probe.calls) != 0 {
		t.Fatalf("malformed target reached storage probe: %+v", probe.calls)
	}
}

func TestScopeRepairObjectAdapterPreservesUpdateMergeContract(t *testing.T) {
	name := "existing.txt"
	description := "keep me"
	controlled := []string{"/programs/org/projects/project"}
	existing := objects.Record{
		Id:               "did-1",
		Name:             &name,
		Description:      &description,
		ControlledAccess: &controlled,
		Properties:       map[string]json.RawMessage{"keep": json.RawMessage(`"value"`)},
	}
	service := &repairObjectServiceFake{record: existing}
	adapter := scopeRepairIndexAdapter{service: service}
	updatedAccess := []objects.AccessMethod{{Type: "s3", AccessUrl: &objects.AccessURL{Url: "s3://bucket/new"}}}
	if err := adapter.Update(context.Background(), "did-1", objects.Record{AccessMethods: &updatedAccess}); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if service.getMethod != "update" {
		t.Fatalf("GetObject method = %q, want update", service.getMethod)
	}
	if len(service.replacements) != 1 {
		t.Fatalf("replacements = %+v", service.replacements)
	}
	merged := service.replacements[0]
	if merged.Id != "did-1" || merged.Name == nil || *merged.Name != name || merged.Description == nil || *merged.Description != description {
		t.Fatalf("immutable/omitted fields were not preserved: %+v", merged)
	}
	if merged.UpdatedTime == nil || merged.UpdatedTime.After(time.Now().UTC()) {
		t.Fatalf("updated timestamp = %v", merged.UpdatedTime)
	}
	if merged.AccessMethods == nil || len(*merged.AccessMethods) != 1 || (*merged.AccessMethods)[0].AccessUrl.Url != "s3://bucket/new" {
		t.Fatalf("access methods = %+v", merged.AccessMethods)
	}
	if string(merged.Properties["keep"]) != `"value"` {
		t.Fatalf("properties = %+v", merged.Properties)
	}
}

func TestScopeRepairObjectAdapterUsesPreparedPaginationAndCollapsePorts(t *testing.T) {
	service := &repairObjectServiceFake{record: objects.Record{Id: "did-1"}}
	adapter := scopeRepairIndexAdapter{service: service}
	records, err := adapter.ListPrepared(context.Background(), scoperepair.PreparedRecordQuery{
		Organization: " org ", Project: " project ", Start: " did-0 ", Limit: 17,
	})
	if err != nil {
		t.Fatalf("ListPrepared() error = %v", err)
	}
	if len(records) != 1 || len(service.queries) != 1 {
		t.Fatalf("records/queries = %+v/%+v", records, service.queries)
	}
	query := service.queries[0]
	if query.organization != "org" || query.project != "project" || query.method != "read" || query.start != "did-0" || query.limit != 17 || query.offset != 0 {
		t.Fatalf("query = %+v", query)
	}
	if _, err := adapter.Collapse(context.Background(), "org", "project"); err != nil {
		t.Fatalf("Collapse() error = %v", err)
	}
	if len(service.collapse) != 1 || service.collapse[0] != "org/project" {
		t.Fatalf("collapse calls = %v", service.collapse)
	}
}

func TestScopeRepairBucketsAdapterFiltersCredentialOrBucketAlias(t *testing.T) {
	adapter := scopeRepairBucketsAdapter{service: repairBucketServiceFake{
		credentials: []buckets.Credential{{CredentialID: "cred-1", Bucket: "bucket-1"}},
		scopes: []buckets.Scope{
			{Bucket: "bucket-1", Organization: "org", ProjectID: "project"},
			{CredentialID: "cred-1", Organization: "org", ProjectID: "alias"},
			{Bucket: "other", Organization: "org", ProjectID: "other"},
		},
	}}
	credentials, err := adapter.ListCredentials(context.Background())
	if err != nil || len(credentials) != 1 {
		t.Fatalf("credentials = %+v, err = %v", credentials, err)
	}
	scopes, err := adapter.ListScopes(context.Background(), "cred-1")
	if err != nil || len(scopes) != 1 || scopes[0].ProjectID != "alias" {
		t.Fatalf("scopes = %+v, err = %v", scopes, err)
	}
}

func TestScopeRepairApplyRejectsReadOnlyProjectUser(t *testing.T) {
	service := scoperepair.NewService(nil, nil, nil, nil, nil)
	resource := "/programs/org/projects/proj"
	req := httptest.NewRequest(http.MethodPost, "/data/repair/project-scope/apply", strings.NewReader(`{"organization":"org","project":"proj"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(policyTestContext("gen3", true, map[string]map[string]bool{
		resource: {"read": true},
	}))
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(req.Context())
		return c.Next()
	})
	app.Post("/data/repair/project-scope/apply", handleInternalScopeRepairApplyFiber(service))
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for read-only caller, got %d", resp.StatusCode)
	}
}
