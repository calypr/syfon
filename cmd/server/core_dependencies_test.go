package server

import (
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/testutils"
)

func mockServerDependencies(database *testutils.MockDatabase) core.Dependencies {
	return core.Dependencies{
		Objects: core.ObjectPorts{
			Reader: database, Writer: database, AccessMethods: database, AccessPolicy: database,
			Aliases: database, Content: database, ChecksumScope: database, Scope: database,
			Resources: database,
		},
		Buckets: core.BucketPorts{
			Credentials: database, CredentialAdmin: database, Scopes: database,
		},
		Transfers: core.TransferPorts{Pending: database, Events: database},
		Usage:     core.UsagePorts{Counters: database, ProviderEvents: database},
	}
}
