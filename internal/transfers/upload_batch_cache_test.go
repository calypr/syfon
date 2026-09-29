package transfers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
)

func uploadBatchContext(projects ...string) context.Context {
	privileges := make(map[string]map[string]bool, len(projects))
	for _, project := range projects {
		privileges["/organization/org/project/"+project] = map[string]bool{"create": true}
	}
	session := access.NewSession("local")
	session.AuthzEnforced = true
	session.SetAuthorizations(nil, privileges, true)
	return access.WithSession(context.Background(), session)
}

func TestUploadBulkReusesTwoScopesAndPreservesMixedFailures(t *testing.T) {
	objectDenied := errors.New("object update denied")
	signDenied := errors.New("signing denied")
	scopeFailure := errors.New("scope unavailable")
	objects := &bulkObjectsFake{errors: map[string]error{
		"a1": errorapi.ErrObjectNotFound, "a2": errorapi.ErrObjectNotFound,
		"b1": errorapi.ErrObjectNotFound, "denied": objectDenied, "bad": errorapi.ErrObjectNotFound,
	}}
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
		"bucket": {Provider: "s3", Bucket: "bucket", AccessKey: "access"},
	}}
	provider := &bulkAccessProvider{errors: map[string]error{"org-prefix/a-prefix/a2": signDenied}}
	events := &eventFake{}
	service := bulkAccessService(t, objects, lookup, provider, events)
	scopes := &countingScopeReader{
		scopes: map[string]buckets.Scope{
			"org|":  {Organization: "org", Bucket: "bucket", PathPrefix: "org-prefix"},
			"org|a": {Organization: "org", ProjectID: "a", Bucket: "bucket", PathPrefix: "a-prefix"},
			"org|b": {Organization: "org", ProjectID: "b", Bucket: "bucket", PathPrefix: "b-prefix"},
		},
		errors: map[string]error{"org|bad": scopeFailure}, calls: map[string]int{},
	}
	service.scopes = scopes
	request := func(id, project string) UploadRequest {
		return UploadRequest{ObjectID: id, Key: id, Scope: &AccessScope{Organization: "org", Project: project}}
	}
	results := service.UploadBulk(uploadBatchContext("a", "b", "bad"), []UploadRequest{
		request("a1", "a"), request("b1", "b"), request("denied", "a"), request("a2", "a"), request("bad", "bad"), request("blocked", "blocked"),
	})
	if len(results) != 6 || results[0].Err != nil || results[1].Err != nil ||
		!errors.Is(results[2].Err, objectDenied) || !errors.Is(results[3].Err, signDenied) ||
		!errors.Is(results[4].Err, scopeFailure) || !errors.Is(results[5].Err, errorapi.ErrAccessDenied) {
		t.Fatalf("mixed upload results=%+v", results)
	}
	if results[0].Target.Key != "org-prefix/a-prefix/a1" || results[1].Target.Key != "org-prefix/b-prefix/b1" {
		t.Fatalf("successful targets=%+v %+v", results[0].Target, results[1].Target)
	}
	for i, id := range []string{"a1", "b1", "denied", "a2", "bad", "blocked"} {
		if results[i].ObjectID != id {
			t.Fatalf("result[%d] ObjectID=%q", i, results[i].ObjectID)
		}
	}
	if got, want := scopes.calls, map[string]int{"org|": 1, "org|a": 1, "org|b": 1, "org|bad": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("scope reads=%v, want %v", got, want)
	}
	if len(lookup.calls) != 1 || len(provider.requests) != 3 || len(events.events) != 0 || len(objects.reads) != 5 {
		t.Fatalf("credential=%d signed=%d events=%d objectReads=%d", len(lookup.calls), len(provider.requests), len(events.events), len(objects.reads))
	}
}

func TestUploadBulkRefreshesScopeAndCredentialAcrossBatches(t *testing.T) {
	objects := &bulkObjectsFake{errors: map[string]error{"one": errorapi.ErrObjectNotFound}}
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
		"bucket": {Provider: "s3", Bucket: "bucket", AccessKey: "first"},
	}}
	provider := &bulkAccessProvider{}
	service := bulkAccessService(t, objects, lookup, provider, &eventFake{})
	scopes := &countingScopeReader{scopes: map[string]buckets.Scope{
		"org|":  {Organization: "org", Bucket: "bucket", PathPrefix: "org-prefix"},
		"org|a": {Organization: "org", ProjectID: "a", Bucket: "bucket", PathPrefix: "old"},
	}, calls: map[string]int{}}
	service.scopes = scopes
	req := UploadRequest{ObjectID: "one", Key: "one", Scope: &AccessScope{Organization: "org", Project: "a"}}
	first := service.UploadBulk(uploadBatchContext("a"), []UploadRequest{req})
	if len(first) != 1 || first[0].Err != nil || first[0].Target.Key != "org-prefix/old/one" {
		t.Fatalf("first batch=%+v", first)
	}
	scopes.scopes["org|a"] = buckets.Scope{Organization: "org", ProjectID: "a", Bucket: "bucket", PathPrefix: "new"}
	lookup.credentials["bucket"] = &buckets.Credential{Provider: "s3", Bucket: "bucket", AccessKey: "second"}
	second := service.UploadBulk(uploadBatchContext("a"), []UploadRequest{req})
	if len(second) != 1 || second[0].Err != nil || second[0].Target.Key != "org-prefix/new/one" ||
		provider.bindings[0].Credential.AccessKey != "first" || provider.bindings[1].Credential.AccessKey != "second" {
		t.Fatalf("second batch=%+v bindings=%+v", second, provider.bindings)
	}
	if scopes.calls["org|"] != 2 || scopes.calls["org|a"] != 2 || len(lookup.calls) != 2 {
		t.Fatalf("scope=%v credential=%v", scopes.calls, lookup.calls)
	}
}

func TestUploadBulkKeepsPerItemExistingObjectEvents(t *testing.T) {
	obj := testRecord()
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{"bucket": {Provider: "s3", Bucket: "bucket", AccessKey: "access"}}}
	provider := &bulkAccessProvider{}
	events := &eventFake{}
	service := bulkAccessService(t, &bulkObjectsFake{objects: map[string]*drs.DrsObject{obj.Id: obj}}, lookup, provider, events)
	scopes := &countingScopeReader{scopes: map[string]buckets.Scope{
		"org|":        {Organization: "org", Bucket: "bucket"},
		"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket"},
	}, calls: map[string]int{}}
	service.scopes = scopes
	requests := []UploadRequest{
		{ObjectID: obj.Id, Key: "first", Scope: &AccessScope{Organization: "org", Project: "project"}},
		{ObjectID: obj.Id, Key: "second", Scope: &AccessScope{Organization: "org", Project: "project"}},
	}
	results := service.UploadBulk(uploadBatchContext("project"), requests)
	if len(results) != 2 || results[0].Err != nil || results[1].Err != nil || !results[0].Existing || !results[1].Existing ||
		results[0].Target.Key != "first" || results[1].Target.Key != "second" || len(events.events) != 2 ||
		len(provider.requests) != 2 || len(lookup.calls) != 1 || scopes.calls["org|"] != 1 || scopes.calls["org|project"] != 1 {
		t.Fatalf("existing upload results=%+v events=%d signed=%d creds=%d scopes=%v", results, len(events.events), len(provider.requests), len(lookup.calls), scopes.calls)
	}
}

type cancelAfterFirstUploadSign struct {
	*bulkAccessProvider
	cancel context.CancelFunc
}

func (p *cancelAfterFirstUploadSign) Sign(ctx context.Context, binding storage.ProviderBinding, req storage.SignRequest) (storage.SignedAccess, error) {
	signed, err := p.bulkAccessProvider.Sign(ctx, binding, req)
	if len(p.requests) == 1 {
		p.cancel()
	}
	return signed, err
}

func TestUploadBulkWarmScopeCacheHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(uploadBatchContext("a"))
	defer cancel()
	objects := &bulkObjectsFake{errors: map[string]error{"one": errorapi.ErrObjectNotFound, "two": errorapi.ErrObjectNotFound}}
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{"bucket": {Provider: "s3", Bucket: "bucket", AccessKey: "access"}}}
	provider := &cancelAfterFirstUploadSign{bulkAccessProvider: &bulkAccessProvider{}, cancel: cancel}
	manager, err := storage.NewManager(lookup, storage.NewRegistration("s3", provider))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	scopes := &countingScopeReader{scopes: map[string]buckets.Scope{
		"org|":  {Organization: "org", Bucket: "bucket"},
		"org|a": {Organization: "org", ProjectID: "a", Bucket: "bucket"},
	}, calls: map[string]int{}}
	service := NewService(Dependencies{Objects: objects, Storage: manager, Scopes: scopes, Events: &eventFake{}})
	requests := []UploadRequest{
		{ObjectID: "one", Key: "one", Scope: &AccessScope{Organization: "org", Project: "a"}},
		{ObjectID: "two", Key: "two", Scope: &AccessScope{Organization: "org", Project: "a"}},
	}
	results := service.UploadBulk(ctx, requests)
	if len(results) != 2 || results[0].Err != nil || !errors.Is(results[1].Err, context.Canceled) ||
		len(provider.requests) != 1 || len(lookup.calls) != 1 || scopes.calls["org|"] != 1 || scopes.calls["org|a"] != 1 {
		t.Fatalf("canceled batch results=%+v signed=%d creds=%d scopes=%v", results, len(provider.requests), len(lookup.calls), scopes.calls)
	}
}
