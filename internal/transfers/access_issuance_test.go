package transfers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/storage"
)

type bulkObjectRead struct {
	objectID string
	action   string
}

type bulkObjectsFake struct {
	objects map[string]*drs.DrsObject
	errors  map[string]error
	reads   []bulkObjectRead
}

func (f *bulkObjectsFake) GetObject(_ context.Context, objectID, action string) (*drs.DrsObject, error) {
	f.reads = append(f.reads, bulkObjectRead{objectID: objectID, action: action})
	if err := f.errors[objectID]; err != nil {
		return nil, err
	}
	return f.objects[objectID], nil
}

func (*bulkObjectsFake) GetObjectsByChecksums(context.Context, []string, string) (map[string][]drs.DrsObject, error) {
	return nil, nil
}

type bulkCredentialLookup struct {
	credentials map[string]*buckets.Credential
	calls       []string
}

func (f *bulkCredentialLookup) GetS3Credential(_ context.Context, candidate string) (*buckets.Credential, error) {
	f.calls = append(f.calls, candidate)
	return f.credentials[candidate], nil
}

type bulkAccessProvider struct {
	bindings []storage.ProviderBinding
	requests []storage.SignRequest
	errors   map[string]error
}

func (f *bulkAccessProvider) Sign(_ context.Context, binding storage.ProviderBinding, request storage.SignRequest) (storage.SignedAccess, error) {
	f.bindings = append(f.bindings, binding)
	f.requests = append(f.requests, request)
	return storage.SignedAccess{Location: "signed-" + request.Target.Key}, f.errors[request.Target.Key]
}

func (*bulkAccessProvider) BeginMultipart(context.Context, storage.ProviderBinding, storage.BeginMultipartRequest) (storage.UploadID, error) {
	return "upload", nil
}

func (*bulkAccessProvider) SignMultipartPart(context.Context, storage.ProviderBinding, storage.MultipartPartRequest) (storage.SignedAccess, error) {
	return storage.SignedAccess{}, nil
}

func (*bulkAccessProvider) CompleteMultipart(context.Context, storage.ProviderBinding, storage.CompleteMultipartRequest) error {
	return nil
}

func bulkAccessObject(id, url string) *drs.DrsObject {
	object := testRecord()
	object.Id = id
	if url == "" {
		object.AccessMethods = nil
		return object
	}
	accessID := "s3"
	methods := []drs.AccessMethod{{AccessId: &accessID, Type: "s3", AccessUrl: &drs.AccessURL{Url: url}}}
	object.AccessMethods = &methods
	return object
}

func bulkAccessService(t *testing.T, objects *bulkObjectsFake, lookup *bulkCredentialLookup, provider *bulkAccessProvider, events *eventFake) *Service {
	t.Helper()
	manager, err := storage.NewManager(lookup, storage.NewRegistration("s3", provider))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return NewService(Dependencies{Objects: objects, Storage: manager, Events: events})
}

func TestIssueAccessBulkPreservesPerObjectAuthorizationAndFailures(t *testing.T) {
	deniedErr := errors.New("object is not readable")
	signErr := errors.New("provider rejected signing")
	objects := &bulkObjectsFake{
		objects: map[string]*drs.DrsObject{
			"one":              bulkAccessObject("one", "s3://shared/object-one"),
			"two":              bulkAccessObject("two", "s3://shared/object-two"),
			"missing-location": bulkAccessObject("missing-location", ""),
			"sign-failed":      bulkAccessObject("sign-failed", "s3://shared/fail"),
		},
		errors: map[string]error{"denied": deniedErr},
	}
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
		"shared": {Provider: "s3", Bucket: "shared", AccessKey: "access"},
	}}
	provider := &bulkAccessProvider{errors: map[string]error{"fail": signErr}}
	events := &eventFake{}
	service := bulkAccessService(t, objects, lookup, provider, events)

	result := service.IssueAccessBulk(context.Background(), []AccessLookupRequest{
		{ObjectID: "one", AccessID: "s3"},
		{ObjectID: "two", AccessID: "s3"},
		{ObjectID: "denied", AccessID: "s3"},
		{ObjectID: "missing-location", AccessID: "s3"},
		{ObjectID: "sign-failed", AccessID: "s3"},
	})
	if result.Requested != 5 || len(result.Resolved) != 2 || len(result.Failures) != 3 {
		t.Fatalf("bulk result = %+v, want 5 requested, 2 resolved, and 3 failures", result)
	}
	failures := make(map[string]error, len(result.Failures))
	for _, failure := range result.Failures {
		failures[failure.ObjectID] = failure.Err
	}
	if !errors.Is(failures["denied"], deniedErr) {
		t.Fatalf("authorization failure = %v, want %v", failures["denied"], deniedErr)
	}
	if !errors.Is(failures["missing-location"], errorapi.ErrObjectLocationUnavailable) {
		t.Fatalf("missing location failure = %v, want ErrObjectLocationUnavailable", failures["missing-location"])
	}
	if !errors.Is(failures["sign-failed"], signErr) {
		t.Fatalf("sign failure = %v, want %v", failures["sign-failed"], signErr)
	}
	if got, want := objects.reads, []bulkObjectRead{
		{objectID: "one", action: "read"},
		{objectID: "two", action: "read"},
		{objectID: "denied", action: "read"},
		{objectID: "missing-location", action: "read"},
		{objectID: "sign-failed", action: "read"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("object authorization reads = %#v, want %#v", got, want)
	}
	if got, want := lookup.calls, []string{"shared"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("credential lookup calls = %#v, want one read for the repeated key %#v", got, want)
	}
	if len(provider.requests) != 3 {
		t.Fatalf("provider sign requests = %d, want successful objects and the independent sign failure", len(provider.requests))
	}
	if len(events.events) != 2 {
		t.Fatalf("transfer events = %d, want one event per successful object", len(events.events))
	}
}

func TestIssueAccessBulkRefreshesCredentialForNextBatch(t *testing.T) {
	objects := &bulkObjectsFake{objects: map[string]*drs.DrsObject{
		"one": bulkAccessObject("one", "s3://shared/object-one"),
		"two": bulkAccessObject("two", "s3://shared/object-two"),
	}}
	lookup := &bulkCredentialLookup{credentials: map[string]*buckets.Credential{
		"shared": {Provider: "s3", Bucket: "shared", AccessKey: "old-key"},
	}}
	provider := &bulkAccessProvider{}
	service := bulkAccessService(t, objects, lookup, provider, &eventFake{})

	first := service.IssueAccessBulk(context.Background(), []AccessLookupRequest{
		{ObjectID: "one", AccessID: "s3"},
		{ObjectID: "two", AccessID: "s3"},
	})
	if len(first.Resolved) != 2 || len(first.Failures) != 0 {
		t.Fatalf("first bulk result = %+v, want two resolved objects", first)
	}
	if got, want := lookup.calls, []string{"shared"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("first batch credential lookups = %#v, want %#v", got, want)
	}

	lookup.credentials["shared"] = &buckets.Credential{Provider: "s3", Bucket: "shared", AccessKey: "rotated-key"}
	second := service.IssueAccessBulk(context.Background(), []AccessLookupRequest{{ObjectID: "one", AccessID: "s3"}})
	if len(second.Resolved) != 1 || len(second.Failures) != 0 {
		t.Fatalf("second bulk result = %+v, want one resolved object", second)
	}
	if got, want := lookup.calls, []string{"shared", "shared"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("credential lookups across batches = %#v, want a fresh lookup per batch %#v", got, want)
	}
	if got, want := []string{
		provider.bindings[0].Credential.AccessKey,
		provider.bindings[1].Credential.AccessKey,
		provider.bindings[2].Credential.AccessKey,
	}, []string{"old-key", "old-key", "rotated-key"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("provider credential rotation = %#v, want %#v", got, want)
	}
}
