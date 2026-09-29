package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
	providerstorage "github.com/calypr/syfon/internal/storage"
)

type projectDeleteBindingProvider struct {
	providerstorage.Provider
	credentialIDs []string
	deleted       []providerstorage.PhysicalTarget
}

func (p *projectDeleteBindingProvider) Delete(_ context.Context, binding providerstorage.ProviderBinding, targets []providerstorage.PhysicalTarget) error {
	p.credentialIDs = append(p.credentialIDs, binding.Credential.CredentialID)
	p.deleted = append(p.deleted, targets...)
	return nil
}

func TestDeleteProjectObjectsRefreshesCredentialCacheNextOperation(t *testing.T) {
	credential := buckets.Credential{CredentialID: "first", Provider: "s3", Bucket: "bucket"}
	reader := &deleteProfileCredentials{value: credential}
	provider := &projectDeleteBindingProvider{}
	manager, err := providerstorage.NewManager(reader, providerstorage.NewRegistration("s3", provider))
	if err != nil {
		t.Fatal(err)
	}
	service := NewService(Dependencies{
		ScopeResolver: fakeScopeResolver{scope: buckets.StorageScope{
			Provider: "s3", Bucket: "bucket", Prefix: "prefix/project", Credential: credential,
		}},
		Credentials: reader,
		Visibility: &fakeVisibility{values: map[string]buckets.VisibleBucket{
			"first": {Credential: credentialMetadata(credential)},
		}},
		Providers: Providers{Delete: manager},
	})
	urls := []string{"s3://bucket/prefix/project/one", "s3://bucket/prefix/project/two"}
	for operation := 1; operation <= 2; operation++ {
		results := service.DeleteProjectObjects(context.Background(), "org", "project", urls)
		if len(results) != 2 || results[0].Status != "deleted" || results[1].Status != "deleted" {
			t.Fatalf("operation %d results = %+v", operation, results)
		}
		if got, want := reader.lookups, 2*operation; got != want {
			t.Fatalf("operation %d credential lookups = %d, want %d", operation, got, want)
		}
		reader.value.CredentialID = "rotated"
	}
	if !reflect.DeepEqual(provider.credentialIDs, []string{"first", "first", "rotated", "rotated"}) {
		t.Fatalf("bound credentials = %v", provider.credentialIDs)
	}
	if !reflect.DeepEqual(provider.deleted, []providerstorage.PhysicalTarget{
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "prefix/project/one"},
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "prefix/project/two"},
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "prefix/project/one"},
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "prefix/project/two"},
	}) {
		t.Fatalf("deleted targets = %+v", provider.deleted)
	}
}
