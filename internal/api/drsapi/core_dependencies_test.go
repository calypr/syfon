package drsapi

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/persistence/sqlite"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

type testObjectManagerFixture struct {
	*core.ObjectManager
	objectService   *objects.Service
	transferService *transfers.Service
}

func testObjectManager(backend any, storagePorts core.StoragePorts) *testObjectManagerFixture {
	deps := testDependencies(backend)
	deps.Storage = storagePorts
	return &testObjectManagerFixture{
		ObjectManager: core.NewObjectManager(deps),
		objectService: newObjectService(deps.Objects),
		transferService: transfers.NewService(transfers.Dependencies{
			Access:      storagePorts.Access,
			Multipart:   storagePorts.Multipart,
			Scopes:      deps.BucketService,
			Credentials: deps.BucketService,
			Events:      deps.Transfers.Events,
		}),
	}
}

func newObjectService(ports core.ObjectPorts) *objects.Service {
	return objects.NewService(objects.Dependencies{
		Reader:        ports.Reader,
		Writer:        ports.Writer,
		AccessMethods: ports.AccessMethods,
		AccessPolicy:  ports.AccessPolicy,
		Aliases:       ports.Aliases,
		Content:       ports.Content,
		ChecksumScope: ports.ChecksumScope,
		Scope:         ports.Scope,
		Resources:     ports.Resources,
		Pages:         ports.Pages,
		URLPages:      ports.URLPages,
		Authorized:    ports.Authorized,
	})
}

func testDependencies(backend any) core.Dependencies {
	var (
		reader          objects.RecordReader
		writer          objects.RecordWriter
		accessMethods   objects.AccessMethodWriter
		accessPolicy    objects.AccessPolicyWriter
		aliases         objects.AliasStore
		content         objects.ContentReader
		checksumScope   objects.ChecksumScopeQuery
		scope           objects.ScopeQuery
		resources       objects.OptionalResourceQuery
		pages           objects.OptionalPageQuery
		urlPages        objects.OptionalURLQuery
		authorized      objects.OptionalAuthorizedQuery
		credentials     buckets.CredentialReader
		credentialAdmin buckets.CredentialAdmin
		scopes          buckets.ScopeStore
		visibility      buckets.VisibilityQuery
		pending         transfers.PendingStore
		events          transfers.EventRecorder
		counters        usage.FileCounterRecorder
		providerEvents  usage.ProviderEventRecorder
	)

	switch db := backend.(type) {
	case *testutils.MockDatabase:
		reader, writer, accessMethods, accessPolicy = db, db, db, db
		aliases, content, checksumScope, scope = db, db, db, db
		resources = db
		credentials, credentialAdmin, scopes = db, db, db
		pending, events, counters, providerEvents = db, db, db, db
	case *sqlite.SqliteDB:
		reader, writer, accessMethods, accessPolicy = db, db, db, db
		aliases, content, checksumScope, scope = db, db, db, db
		resources, pages, urlPages, authorized = db, db, db, db
		credentials, credentialAdmin, scopes, visibility = db, db, db, db
		pending, events, counters, providerEvents = db, db, db, db
	default:
		panic("unsupported DRS test backend")
	}
	if optional, ok := backend.(objects.OptionalPageQuery); ok {
		pages = optional
	}
	if optional, ok := backend.(objects.OptionalURLQuery); ok {
		urlPages = optional
	}
	if optional, ok := backend.(objects.OptionalAuthorizedQuery); ok {
		authorized = optional
	}
	if optional, ok := backend.(buckets.VisibilityQuery); ok {
		visibility = optional
	}

	objectPorts := core.ObjectPorts{
		Reader: reader, Writer: writer, AccessMethods: accessMethods, AccessPolicy: accessPolicy,
		Aliases: aliases, Content: content, ChecksumScope: checksumScope, Scope: scope,
		Resources: resources, Pages: pages, URLPages: urlPages, Authorized: authorized,
	}
	bucketService, err := buckets.NewService(buckets.Dependencies{
		Credentials: credentials, CredentialAdmin: credentialAdmin, Scopes: scopes, Visibility: visibility,
		Fallback: core.NewBucketVisibilityFallback(scope, reader),
	}, nil)
	if err != nil {
		panic(err)
	}
	return core.Dependencies{
		Objects:       objectPorts,
		BucketService: bucketService,
		Transfers:     core.TransferPorts{Pending: pending, Events: events},
		Usage:         core.UsagePorts{Counters: counters, ProviderEvents: providerEvents},
	}
}
