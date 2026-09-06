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
	var visibility buckets.VisibilityQuery
	if port, ok := interface{}(db).(buckets.VisibilityQuery); ok {
		visibility = port
	}
	service, err := buckets.NewService(buckets.Dependencies{
		Credentials: db, CredentialAdmin: db, Scopes: db, Visibility: visibility,
		Fallback: core.NewBucketVisibilityFallback(deps.Objects.Scope, deps.Objects.Reader),
	}, nil)
	if err != nil {
		panic(err)
	}
	deps.BucketService = service

	return deps
}

func newLFSObjectService(deps core.Dependencies) *objects.Service {
	return objects.NewService(objects.Dependencies{
		Reader:        deps.Objects.Reader,
		Writer:        deps.Objects.Writer,
		AccessMethods: deps.Objects.AccessMethods,
		AccessPolicy:  deps.Objects.AccessPolicy,
		Aliases:       deps.Objects.Aliases,
		Content:       deps.Objects.Content,
		ChecksumScope: deps.Objects.ChecksumScope,
		Scope:         deps.Objects.Scope,
		Resources:     deps.Objects.Resources,
		Pages:         deps.Objects.Pages,
		URLPages:      deps.Objects.URLPages,
		Authorized:    deps.Objects.Authorized,
	})
}

var _ transfers.PendingStore = (*testutils.MockDatabase)(nil)
var _ transfers.EventRecorder = (*testutils.MockDatabase)(nil)
var _ usage.FileCounterRecorder = (*testutils.MockDatabase)(nil)
var _ usage.ProviderEventRecorder = (*testutils.MockDatabase)(nil)
