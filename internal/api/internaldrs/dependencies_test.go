package internaldrs

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/calypr/syfon/internal/usage"
)

func newInternalDRSObjectManager(store any, manager urlmanager.UrlManager) *core.ObjectManager {
	return core.NewObjectManager(core.Dependencies{
		Objects: core.ObjectPorts{
			Reader:        store.(objects.RecordReader),
			Writer:        store.(objects.RecordWriter),
			AccessMethods: store.(objects.AccessMethodWriter),
			AccessPolicy:  store.(objects.AccessPolicyWriter),
			Aliases:       store.(objects.AliasStore),
			Content:       store.(objects.ContentReader),
			ChecksumScope: store.(objects.ChecksumScopeQuery),
			Scope:         store.(objects.ScopeQuery),
			Resources:     optionalInternalDRSPort[objects.OptionalResourceQuery](store),
			Pages:         optionalInternalDRSPort[objects.OptionalPageQuery](store),
			URLPages:      optionalInternalDRSPort[objects.OptionalURLQuery](store),
			Authorized:    optionalInternalDRSPort[objects.OptionalAuthorizedQuery](store),
		},
		Buckets: core.BucketPorts{
			Credentials:     store.(buckets.CredentialReader),
			CredentialAdmin: store.(buckets.CredentialAdmin),
			Scopes:          store.(buckets.ScopeStore),
			Visibility:      optionalInternalDRSPort[buckets.VisibilityQuery](store),
		},
		Transfers: core.TransferPorts{
			Pending: store.(transfers.PendingStore),
			Events:  store.(transfers.EventRecorder),
		},
		Usage: core.UsagePorts{
			Counters:       store.(usage.FileCounterRecorder),
			ProviderEvents: store.(usage.ProviderEventRecorder),
		},
	}, manager)
}

func optionalInternalDRSPort[T any](store any) T {
	value, _ := store.(T)
	return value
}
