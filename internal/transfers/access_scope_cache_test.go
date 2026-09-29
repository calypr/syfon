package transfers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/buckets"
)

type countingScopeReader struct {
	scopes map[string]buckets.Scope
	errors map[string]error
	calls  map[string]int
}

func (r *countingScopeReader) LookupBucketScope(_ context.Context, organization, project string) (buckets.Scope, bool, error) {
	key := organization + "|" + project
	r.calls[key]++
	if err := r.errors[key]; err != nil {
		return buckets.Scope{}, false, err
	}
	scope, found := r.scopes[key]
	return scope, found, nil
}

func TestIssueAccessBulkReusesScopesOnlyWithinBatch(t *testing.T) {
	makeObject := func(id, project string) *drs.DrsObject {
		obj := bulkAccessObject(id, "s3://physical/"+id)
		resources := []string{"/organization/org/project/" + project}
		obj.ControlledAccess = &resources
		return obj
	}
	objects := &bulkObjectsFake{objects: map[string]*drs.DrsObject{
		"a1":               makeObject("a1", "a"),
		"a2":               makeObject("a2", "a"),
		"b1":               makeObject("b1", "b"),
		"none1":            makeObject("none1", "none"),
		"none2":            makeObject("none2", "none"),
		"bad1":             makeObject("bad1", "bad"),
		"bad2":             makeObject("bad2", "bad"),
		"missing-location": bulkAccessObject("missing-location", ""),
	}}
	scopeError := errors.New("scope unavailable")
	scopes := &countingScopeReader{
		scopes: map[string]buckets.Scope{
			"org|":  {Organization: "org", Bucket: "physical", PathPrefix: "org-prefix"},
			"org|a": {Organization: "org", ProjectID: "a", Bucket: "physical", PathPrefix: "a-prefix"},
			"org|b": {Organization: "org", ProjectID: "b", Bucket: "physical", PathPrefix: "b-prefix"},
		},
		errors: map[string]error{"org|bad": scopeError},
		calls:  map[string]int{},
	}
	provider := &bulkAccessProvider{}
	events := &eventFake{}
	service := bulkAccessService(t, objects, &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
		"physical": {Provider: "s3", Bucket: "physical", AccessKey: "access"},
	}}, provider, events)
	service.scopes = scopes
	requests := []AccessLookupRequest{
		{ObjectID: "a1", AccessID: "s3"},
		{ObjectID: "a2", AccessID: "s3"},
		{ObjectID: "b1", AccessID: "s3"},
		{ObjectID: "none1", AccessID: "s3"},
		{ObjectID: "none2", AccessID: "s3"},
		{ObjectID: "bad1", AccessID: "s3"},
		{ObjectID: "bad2", AccessID: "s3"},
		{ObjectID: "missing-location", AccessID: "s3"},
	}
	first := service.IssueAccessBulk(context.Background(), requests)
	if first.Requested != len(requests) || len(first.Resolved) != 5 || len(first.Failures) != 3 {
		t.Fatalf("first result = %+v", first)
	}
	for _, failure := range first.Failures {
		if failure.ObjectID == "missing-location" {
			if !errors.Is(failure.Err, errorapi.ErrObjectLocationUnavailable) {
				t.Fatalf("missing location error = %v", failure.Err)
			}
		} else if !errors.Is(failure.Err, scopeError) {
			t.Fatalf("scope failure for %q = %v", failure.ObjectID, failure.Err)
		}
	}
	if got, want := scopes.calls, map[string]int{"org|": 1, "org|a": 1, "org|b": 1, "org|none": 1, "org|bad": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first batch scope calls = %v, want %v", got, want)
	}
	if got, want := []string{provider.requests[0].Target.Key, provider.requests[1].Target.Key, provider.requests[2].Target.Key}, []string{"org-prefix/a-prefix/a1", "org-prefix/a-prefix/a2", "org-prefix/b-prefix/b1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("project keys = %v, want %v", got, want)
	}
	if len(events.events) != 5 {
		t.Fatalf("events = %d, want 5", len(events.events))
	}

	scopes.scopes["org|a"] = buckets.Scope{Organization: "org", ProjectID: "a", Bucket: "physical", PathPrefix: "fresh"}
	second := service.IssueAccessBulk(context.Background(), []AccessLookupRequest{{ObjectID: "a1", AccessID: "s3"}})
	if len(second.Resolved) != 1 || len(second.Failures) != 0 {
		t.Fatalf("second result = %+v", second)
	}
	if got := provider.requests[len(provider.requests)-1].Target.Key; got != "org-prefix/fresh/a1" {
		t.Fatalf("refreshed key = %q", got)
	}
	if scopes.calls["org|"] != 2 || scopes.calls["org|a"] != 2 {
		t.Fatalf("scope calls across batches = %v", scopes.calls)
	}
}

func TestBatchScopeReaderHonorsCancellationAfterCacheHit(t *testing.T) {
	source := &countingScopeReader{scopes: map[string]buckets.Scope{
		"org|project": {Organization: "org", ProjectID: "project", Bucket: "bucket"},
	}, calls: map[string]int{}}
	reader := &batchScopeReader{source: source, entries: map[scopeKey]scopeLookup{}}
	ctx, cancel := context.WithCancel(context.Background())
	if _, found, err := reader.LookupBucketScope(ctx, "org", "project"); err != nil || !found {
		t.Fatalf("first scope lookup: found=%t err=%v", found, err)
	}
	cancel()
	if _, found, err := reader.LookupBucketScope(ctx, "org", "project"); !errors.Is(err, context.Canceled) || found {
		t.Fatalf("cached lookup after cancellation: found=%t err=%v", found, err)
	}
	if source.calls["org|project"] != 1 {
		t.Fatalf("source calls = %d, want 1", source.calls["org|project"])
	}
}
