package lfs

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

func newLFSDependencies(db *testutils.MockDatabase) core.Dependencies {
	deps := core.Dependencies{
		Objects: core.ObjectPorts{
			Reader:        db,
			Writer:        db,
			AccessMethods: db,
			AccessPolicy:  db,
			Aliases:       db,
			Content:       db,
			ChecksumScope: db,
			Scope:         db,
		},
		Buckets: core.BucketPorts{
			Credentials:     db,
			CredentialAdmin: db,
			Scopes:          db,
		},
		Transfers: core.TransferPorts{
			Pending: db,
			Events:  db,
		},
		Usage: core.UsagePorts{
			Counters:       db,
			ProviderEvents: db,
		},
	}

	if port, ok := interface{}(db).(objects.OptionalResourceQuery); ok {
		deps.Objects.Resources = port
	}
	if port, ok := interface{}(db).(objects.OptionalPageQuery); ok {
		deps.Objects.Pages = port
	}
	if port, ok := interface{}(db).(objects.OptionalURLQuery); ok {
		deps.Objects.URLPages = port
	}
	if port, ok := interface{}(db).(objects.OptionalAuthorizedQuery); ok {
		deps.Objects.Authorized = port
	}
	if port, ok := interface{}(db).(buckets.OptionalVisibilityQuery); ok {
		deps.Buckets.Visibility = port
	}

	return deps
}

var _ transfers.PendingStore = (*testutils.MockDatabase)(nil)
var _ transfers.EventRecorder = (*testutils.MockDatabase)(nil)
var _ usage.FileCounterRecorder = (*testutils.MockDatabase)(nil)
var _ usage.ProviderEventRecorder = (*testutils.MockDatabase)(nil)
