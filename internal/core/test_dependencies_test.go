package core

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

func newTestObjectManager(backend any, storageDependency any) *ObjectManager {
	deps := testDependencies(backend)
	bucketDeps := buckets.Dependencies{
		Credentials:     backend.(buckets.CredentialReader),
		CredentialAdmin: backend.(buckets.CredentialAdmin),
		Scopes:          backend.(buckets.ScopeStore),
		Fallback:        NewBucketVisibilityFallback(deps.Objects.Scope, deps.Objects.Reader),
	}
	if optional, ok := backend.(buckets.VisibilityQuery); ok {
		bucketDeps.Visibility = optional
	}
	var invalidator interface{ InvalidateBucket(string) }
	if candidate, ok := storageDependency.(interface{ InvalidateBucket(string) }); ok {
		invalidator = candidate
	}
	service, err := buckets.NewService(bucketDeps, invalidator)
	if err != nil {
		panic(err)
	}
	deps.BucketService = service
	if candidate, ok := storageDependency.(*capturingURLManager); ok && candidate != nil {
		deps.Storage = StoragePorts{
			Access:    candidate,
			Multipart: candidate,
			Probe:     candidate,
			Inventory: candidate,
			Delete:    candidate,
		}
	}
	if candidate, ok := storageDependency.(StoragePorts); ok {
		deps.Storage = candidate
	}
	return NewObjectManager(deps)
}

var _ StorageAccess = (*capturingURLManager)(nil)
var _ StorageMultipart = (*capturingURLManager)(nil)
var _ StorageProbe = (*capturingURLManager)(nil)
var _ StorageInventory = (*capturingURLManager)(nil)
var _ StorageDelete = (*capturingURLManager)(nil)

// testDependencies composes the capabilities needed by ObjectManager from the
// concrete test backend. Optional interfaces stay nil when a test double does
// not implement them, so the production fallback paths remain exercised.
func testDependencies(backend any) Dependencies {
	deps := Dependencies{
		Objects: ObjectPorts{
			Reader:        backend.(objects.RecordReader),
			Writer:        backend.(objects.RecordWriter),
			AccessMethods: backend.(objects.AccessMethodWriter),
			AccessPolicy:  backend.(objects.AccessPolicyWriter),
			Aliases:       backend.(objects.AliasStore),
			Content:       backend.(objects.ContentReader),
			ChecksumScope: backend.(objects.ChecksumScopeQuery),
			Scope:         backend.(objects.ScopeQuery),
		},
		Transfers: TransferPorts{
			Pending: backend.(transfers.PendingStore),
			Events:  backend.(transfers.EventRecorder),
		},
		Usage: UsagePorts{
			Counters:       backend.(usage.FileCounterRecorder),
			ProviderEvents: backend.(usage.ProviderEventRecorder),
		},
	}
	if optional, ok := backend.(objects.OptionalResourceQuery); ok {
		deps.Objects.Resources = optional
	}
	if optional, ok := backend.(objects.OptionalPageQuery); ok {
		deps.Objects.Pages = optional
	}
	if optional, ok := backend.(objects.OptionalURLQuery); ok {
		deps.Objects.URLPages = optional
	}
	if optional, ok := backend.(objects.OptionalAuthorizedQuery); ok {
		deps.Objects.Authorized = optional
	}
	return deps
}
