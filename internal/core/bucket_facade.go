package core

import (
	"context"

	"github.com/calypr/syfon/internal/buckets"
)

func (m *ObjectManager) ListS3Credentials(ctx context.Context) ([]buckets.Credential, error) {
	return m.bucketService.ListS3Credentials(ctx)
}

func (m *ObjectManager) GetS3Credential(ctx context.Context, credentialID string) (*buckets.Credential, error) {
	return m.bucketService.GetS3Credential(ctx, credentialID)
}

func (m *ObjectManager) SaveS3Credential(ctx context.Context, credential *buckets.Credential) error {
	return m.bucketService.SaveS3Credential(ctx, credential)
}

func (m *ObjectManager) DeleteS3Credential(ctx context.Context, credentialID string) error {
	return m.bucketService.DeleteS3Credential(ctx, credentialID)
}

func (m *ObjectManager) ListBucketScopes(ctx context.Context) ([]buckets.Scope, error) {
	return m.bucketService.ListBucketScopes(ctx)
}

func (m *ObjectManager) CreateBucketScope(ctx context.Context, scope *buckets.Scope) error {
	return m.bucketService.CreateBucketScope(ctx, scope)
}

func (m *ObjectManager) DeleteBucketScope(ctx context.Context, organization, projectID, credentialID, pathPrefix string) error {
	return m.bucketService.DeleteBucketScope(ctx, organization, projectID, credentialID, pathPrefix)
}

func (m *ObjectManager) ListVisibleBuckets(ctx context.Context) (map[string]buckets.VisibleBucket, error) {
	return m.listVisibleBucketsCached(ctx)
}

func (m *ObjectManager) listVisibleBucketsUncached(ctx context.Context) (map[string]buckets.VisibleBucket, error) {
	return m.bucketService.ListVisibleBuckets(ctx)
}
