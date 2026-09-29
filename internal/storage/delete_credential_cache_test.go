package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/calypr/syfon/internal/buckets"
)

type credentialInspectingBackend struct {
	*fakeBackend
	credentialIDs []string
}

func (b *credentialInspectingBackend) Delete(ctx context.Context, binding ProviderBinding, targets []PhysicalTarget) error {
	if binding.Credential != nil {
		b.credentialIDs = append(b.credentialIDs, binding.Credential.CredentialID)
	}
	return b.fakeBackend.Delete(ctx, binding, targets)
}

func TestDeleteExactReusesCredentialWithinOperationAndRefreshesNextOperation(t *testing.T) {
	lookup := &fakeLookup{credentials: map[string]*buckets.Credential{
		"bucket": {CredentialID: "first", Provider: "s3", Bucket: "bucket"},
	}}
	s3 := &credentialInspectingBackend{fakeBackend: &fakeBackend{provider: "s3"}}
	gcs := &credentialInspectingBackend{fakeBackend: &fakeBackend{provider: "gcs"}}
	manager, err := NewManager(lookup, NewRegistration("s3", s3), NewRegistration("gcs", gcs))
	if err != nil {
		t.Fatal(err)
	}
	targets := []DeleteTarget{{Location: "s3://bucket/key-1"}, {Location: "s3://bucket/key-2"}}
	if err := manager.DeleteExact(context.Background(), targets); err != nil {
		t.Fatal(err)
	}
	if len(lookup.queries) != 1 || len(s3.deletions) != 1 || len(s3.deletions[0]) != 2 {
		t.Fatalf("first delete: lookups=%v s3 deletions=%v", lookup.queries, s3.deletions)
	}
	if !reflect.DeepEqual(s3.credentialIDs, []string{"first"}) || !reflect.DeepEqual(s3.deletions[0], []PhysicalTarget{
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "key-1"},
		{Provider: "s3", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "key-2"},
	}) {
		t.Fatalf("first delete binding=%v targets=%v", s3.credentialIDs, s3.deletions[0])
	}
	lookup.credentials["bucket"] = &buckets.Credential{CredentialID: "second", Provider: "gcs", Bucket: "bucket"}
	if err := manager.DeleteExact(context.Background(), targets); err != nil {
		t.Fatal(err)
	}
	if len(lookup.queries) != 2 || len(gcs.deletions) != 1 || len(gcs.deletions[0]) != 2 {
		t.Fatalf("rotated delete: lookups=%v gcs deletions=%v", lookup.queries, gcs.deletions)
	}
	if !reflect.DeepEqual(gcs.credentialIDs, []string{"second"}) || !reflect.DeepEqual(gcs.deletions[0], []PhysicalTarget{
		{Provider: "gcs", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "key-1"},
		{Provider: "gcs", LookupKey: "bucket", PhysicalBucket: "bucket", Key: "key-2"},
	}) {
		t.Fatalf("rotated delete binding=%v targets=%v", gcs.credentialIDs, gcs.deletions[0])
	}
}

func TestDeleteExactPreservesParentCacheAndStopsOnCancellation(t *testing.T) {
	lookup := &fakeLookup{credentials: map[string]*buckets.Credential{
		"bucket": {Provider: "s3", Bucket: "bucket"},
	}}
	backend := &fakeBackend{provider: "s3"}
	manager := managerWithBackends(t, lookup, backend)
	base, cancel := context.WithCancel(context.Background())
	ctx := WithCredentialCache(base)
	target := []DeleteTarget{{Location: "s3://bucket/key"}}
	if err := manager.DeleteExact(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := manager.DeleteExact(ctx, target); err != nil {
		t.Fatal(err)
	}
	if len(lookup.queries) != 1 || len(backend.deletions) != 2 {
		t.Fatalf("parent cache not reused: lookups=%v deletions=%v", lookup.queries, backend.deletions)
	}
	cancel()
	err := manager.DeleteExact(ctx, target)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled warm-cache delete error = %v", err)
	}
	if len(lookup.queries) != 1 || len(backend.deletions) != 2 {
		t.Fatalf("canceled delete performed work: lookups=%v deletions=%v", lookup.queries, backend.deletions)
	}
}

func TestDeleteExactCredentialErrorDoesNotLeakAcrossOperations(t *testing.T) {
	lookupErr := errors.New("credential unavailable")
	lookup := &fakeLookup{errors: map[string]error{"bucket": lookupErr}}
	backend := &fakeBackend{provider: "s3"}
	manager := managerWithBackends(t, lookup, backend)
	target := []DeleteTarget{{Location: "s3://bucket/key"}}
	if err := manager.DeleteExact(context.Background(), target); !errors.Is(err, lookupErr) {
		t.Fatalf("lookup error = %v", err)
	}
	delete(lookup.errors, "bucket")
	lookup.credentials = map[string]*buckets.Credential{"bucket": {Provider: "s3", Bucket: "bucket"}}
	if err := manager.DeleteExact(context.Background(), target); err != nil {
		t.Fatalf("retry after restored credential: %v", err)
	}
	if len(lookup.queries) != 2 || len(backend.deletions) != 1 {
		t.Fatalf("retry lookups=%v deletions=%v", lookup.queries, backend.deletions)
	}
}
