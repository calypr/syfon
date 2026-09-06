package drsapi

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/db/sqlite"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/testutils"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/calypr/syfon/internal/usage"
)

func testObjectManager(backend any, uM urlmanager.UrlManager) *core.ObjectManager {
	return core.NewObjectManager(testDependencies(backend), uM)
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
		visibility      buckets.OptionalVisibilityQuery
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
	if optional, ok := backend.(buckets.OptionalVisibilityQuery); ok {
		visibility = optional
	}

	return core.Dependencies{
		Objects: core.ObjectPorts{
			Reader: reader, Writer: writer, AccessMethods: accessMethods, AccessPolicy: accessPolicy,
			Aliases: aliases, Content: content, ChecksumScope: checksumScope, Scope: scope,
			Resources: resources, Pages: pages, URLPages: urlPages, Authorized: authorized,
		},
		Buckets: core.BucketPorts{
			Credentials: credentials, CredentialAdmin: credentialAdmin, Scopes: scopes, Visibility: visibility,
		},
		Transfers: core.TransferPorts{Pending: pending, Events: events},
		Usage:     core.UsagePorts{Counters: counters, ProviderEvents: providerEvents},
	}
}
