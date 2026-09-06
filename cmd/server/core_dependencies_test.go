package server

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
)

func mockServerDependencies(database *testutils.MockDatabase, storagePorts core.StoragePorts) core.Dependencies {
	objectPorts := core.ObjectPorts{
		Reader: database, Writer: database, AccessMethods: database, AccessPolicy: database,
		Aliases: database, Content: database, ChecksumScope: database, Scope: database,
		Resources: database,
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{
		Credentials: database, CredentialAdmin: database, Scopes: database,
		Fallback: core.NewBucketVisibilityFallback(objectPorts.Scope, objectPorts.Reader),
	}, nil)
	if err != nil {
		panic(err)
	}
	return core.Dependencies{
		Objects:       objectPorts,
		BucketService: bucketService,
		Transfers:     core.TransferPorts{Pending: database, Events: database},
		Usage:         core.UsagePorts{Counters: database, ProviderEvents: database},
		Storage:       storagePorts,
	}
}
