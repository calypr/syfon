package server

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/urlmanager"
)

func mockServerDependencies(database *testutils.MockDatabase, manager urlmanager.UrlManager) core.Dependencies {
	objectPorts := core.ObjectPorts{
		Reader: database, Writer: database, AccessMethods: database, AccessPolicy: database,
		Aliases: database, Content: database, ChecksumScope: database, Scope: database,
		Resources: database,
	}
	var invalidator interface{ InvalidateBucket(string) }
	if candidate, ok := manager.(interface{ InvalidateBucket(string) }); ok {
		invalidator = candidate
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{
		Credentials: database, CredentialAdmin: database, Scopes: database,
		Fallback: core.NewBucketVisibilityFallback(objectPorts.Scope, objectPorts.Reader),
	}, invalidator)
	if err != nil {
		panic(err)
	}
	return core.Dependencies{
		Objects:       objectPorts,
		BucketService: bucketService,
		Transfers:     core.TransferPorts{Pending: database, Events: database},
		Usage:         core.UsagePorts{Counters: database, ProviderEvents: database},
	}
}
