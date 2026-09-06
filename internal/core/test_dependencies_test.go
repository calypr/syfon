package core

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/calypr/syfon/internal/usage"
)

func newTestObjectManager(backend any, uM urlmanager.UrlManager) *ObjectManager {
	return NewObjectManager(testDependencies(backend), uM)
}

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
		Buckets: BucketPorts{
			Credentials:     backend.(buckets.CredentialReader),
			CredentialAdmin: backend.(buckets.CredentialAdmin),
			Scopes:          backend.(buckets.ScopeStore),
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
	if optional, ok := backend.(buckets.VisibilityQuery); ok {
		deps.Buckets.Visibility = optional
	}
	return deps
}
