package core

import (
	"context"
	"fmt"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/testutils"
)

type coreTestDB struct {
	*testutils.MockDatabase
	aliases                map[string]string
	creds                  []buckets.Credential
	getS3CredentialCalls   int
	listS3CredentialsCalls int
}

func (d *coreTestDB) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	if canonical, ok := d.aliases[aliasID]; ok {
		return canonical, nil
	}
	return "", fmt.Errorf("%w: object not found", faults.ErrNotFound)
}

func (d *coreTestDB) ListS3Credentials(ctx context.Context) ([]buckets.Credential, error) {
	d.listS3CredentialsCalls++
	if d.creds != nil {
		return append([]buckets.Credential(nil), d.creds...), nil
	}
	if d.MockDatabase == nil {
		return nil, nil
	}
	return d.MockDatabase.ListS3Credentials(ctx)
}

func (d *coreTestDB) GetS3Credential(ctx context.Context, bucket string) (*buckets.Credential, error) {
	d.getS3CredentialCalls++
	if d.MockDatabase == nil {
		return nil, nil
	}
	return d.MockDatabase.GetS3Credential(ctx, bucket)
}

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
	if candidate, ok := storageDependency.(StoragePorts); ok {
		deps.Storage = candidate
	}
	return NewObjectManager(deps)
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
