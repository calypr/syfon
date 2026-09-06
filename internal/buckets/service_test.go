package buckets

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewServiceRequiresTheCompletePolicyGraph(t *testing.T) {
	credentialStore := &fakeCredentialStore{}
	scopeStore := &fakeScopeStore{}
	visibility := &fakeVisibilityQuery{}
	valid := Dependencies{
		Credentials:     credentialStore,
		CredentialAdmin: credentialStore,
		Scopes:          scopeStore,
		Visibility:      visibility,
	}

	tests := []struct {
		name    string
		deps    Dependencies
		wantErr string
	}{
		{name: "credential reader", deps: Dependencies{CredentialAdmin: credentialStore, Scopes: scopeStore, Visibility: visibility}, wantErr: "credential reader"},
		{name: "credential admin", deps: Dependencies{Credentials: credentialStore, Scopes: scopeStore, Visibility: visibility}, wantErr: "credential admin"},
		{name: "scope store", deps: Dependencies{Credentials: credentialStore, CredentialAdmin: credentialStore, Visibility: visibility}, wantErr: "scope store"},
		{name: "visibility source", deps: Dependencies{Credentials: credentialStore, CredentialAdmin: credentialStore, Scopes: scopeStore}, wantErr: "visibility query or fallback"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, err := NewService(tc.deps, nil)
			if service != nil || err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("NewService()=(%v,%v), want error containing %q", service, err, tc.wantErr)
			}
		})
	}

	service, err := NewService(valid, nil)
	if err != nil || service == nil {
		t.Fatalf("valid NewService()=(%v,%v)", service, err)
	}
	service, err = NewService(Dependencies{
		Credentials:     credentialStore,
		CredentialAdmin: credentialStore,
		Scopes:          scopeStore,
		Fallback:        func(context.Context) ([]VisibilityRow, error) { return nil, nil },
	}, nil)
	if err != nil || service == nil {
		t.Fatalf("fallback-only NewService()=(%v,%v)", service, err)
	}
}

func TestNewServiceNilInvalidatorIsSafe(t *testing.T) {
	service, _, _ := newFakeService(nil, nil, &fakeVisibilityQuery{}, nil, nil)
	if service == nil || service.signerCacheInvalidator != nil {
		t.Fatalf("nil invalidator should be retained as a no-op, service=%v invalidator=%v", service, service.signerCacheInvalidator)
	}
	if err := service.SaveS3Credential(context.Background(), &Credential{Bucket: "bucket-a"}); err != nil {
		t.Fatalf("SaveS3Credential with nil invalidator: %v", err)
	}
}

func TestServiceDelegatesCredentialAndScopeReads(t *testing.T) {
	credential := Credential{CredentialID: "id-a", Bucket: "bucket-a"}
	service, credentials, scopes := newFakeService([]Credential{credential}, []Scope{{Organization: "org", ProjectID: "project"}}, &fakeVisibilityQuery{}, nil, nil)

	gotCredentials, err := service.ListS3Credentials(context.Background())
	if err != nil || len(gotCredentials) != 1 || gotCredentials[0] != credential {
		t.Fatalf("ListS3Credentials()=(%v,%v)", gotCredentials, err)
	}
	gotCredential, err := service.GetS3Credential(context.Background(), "id-a")
	if err != nil || gotCredential == nil || gotCredential.Bucket != "bucket-a" {
		t.Fatalf("GetS3Credential()=(%v,%v)", gotCredential, err)
	}
	gotScopes, err := service.ListBucketScopes(context.Background())
	if err != nil || len(gotScopes) != 1 {
		t.Fatalf("ListBucketScopes()=(%v,%v)", gotScopes, err)
	}
	if credentials.listCalls != 1 || credentials.getCalls != 1 || scopes.listCalls != 1 {
		t.Fatalf("unexpected delegation counts: credentials list=%d get=%d scopes list=%d", credentials.listCalls, credentials.getCalls, scopes.listCalls)
	}
}

func TestResolveBucketPreservesRepositoryOrderAndPhysicalMatching(t *testing.T) {
	first := Credential{CredentialID: "id-first", Bucket: "first-bucket"}
	second := Credential{CredentialID: "id-second", Bucket: "second-bucket"}
	service, _, _ := newFakeService([]Credential{first, second}, nil, &fakeVisibilityQuery{}, nil, nil)

	for _, tc := range []struct {
		name      string
		requested string
		want      string
	}{
		{name: "empty uses first repository result", requested: "", want: "first-bucket"},
		{name: "physical bucket", requested: "second-bucket", want: "second-bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := service.ResolveBucket(context.Background(), tc.requested)
			if err != nil || got != tc.want {
				t.Fatalf("ResolveBucket(%q)=(%q,%v), want %q", tc.requested, got, err, tc.want)
			}
		})
	}
	if _, err := service.ResolveBucket(context.Background(), "id-second"); err == nil {
		t.Fatal("ResolveBucket should match physical bucket names, not credential IDs")
	}
}

func TestResolveBucketReportsEmptyAndUnknownBuckets(t *testing.T) {
	empty, _, _ := newFakeService(nil, nil, &fakeVisibilityQuery{}, nil, nil)
	if _, err := empty.ResolveBucket(context.Background(), ""); err == nil {
		t.Fatal("empty repository should return an error")
	}
	service, _, _ := newFakeService([]Credential{{Bucket: "bucket-a"}}, nil, &fakeVisibilityQuery{}, nil, nil)
	if _, err := service.ResolveBucket(context.Background(), "missing"); err == nil {
		t.Fatal("unknown bucket should return an error")
	}
}

func TestResolveBucketPropagatesRepositoryErrors(t *testing.T) {
	credentialStore := &fakeCredentialStore{listErr: errors.New("database unavailable")}
	service, err := NewService(Dependencies{
		Credentials:     credentialStore,
		CredentialAdmin: credentialStore,
		Scopes:          &fakeScopeStore{},
		Fallback:        func(context.Context) ([]VisibilityRow, error) { return nil, nil },
	}, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if _, err := service.ResolveBucket(context.Background(), ""); !errors.Is(err, credentialStore.listErr) {
		t.Fatalf("ResolveBucket error=%v, want %v", err, credentialStore.listErr)
	}
}
