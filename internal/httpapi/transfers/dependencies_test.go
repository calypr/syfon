package transfers

import (
	"context"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/httpapi/transfers/testutils"
	"github.com/calypr/syfon/internal/objects"
	domaintransfers "github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

type internalDRSTestFixture struct {
	ObjectService   *objects.Service
	TransferService *domaintransfers.Service
	FileCounters    usage.FileCounterRecorder
	bucketService   *buckets.Service
}

func newInternalDRSObjectManager(store any, storageDependency any) internalDRSTestFixture {
	objectStore := testutils.ObjectPortsFor(store)
	bucketService := newInternalDRSBucketService(store)
	storageAccess, _ := storageDependency.(domaintransfers.AccessPort)
	storageMultipart, _ := storageDependency.(domaintransfers.MultipartPort)

	objectService := objects.NewService(objects.Dependencies{
		Reader:        objectStore.Reader,
		Writer:        objectStore.Writer,
		AccessMethods: objectStore.AccessMethods,
		AccessPolicy:  objectStore.AccessPolicy,
		Aliases:       objectStore.Aliases,
		Content:       objectStore.Content,
		ChecksumScope: objectStore.ChecksumScope,
		Scope:         objectStore.Scope,
		Resources:     objectStore.Resources,
		Pages:         objectStore.Pages,
		URLPages:      objectStore.URLPages,
		Authorized:    objectStore.Authorized,
	})
	transferService := domaintransfers.NewService(domaintransfers.Dependencies{
		Access:      storageAccess,
		Multipart:   storageMultipart,
		Scopes:      bucketService,
		Credentials: bucketService,
		Pending:     store.(domaintransfers.PendingStore),
		Events:      store.(domaintransfers.EventRecorder),
	})
	return internalDRSTestFixture{
		ObjectService:   objectService,
		TransferService: transferService,
		FileCounters:    store.(usage.FileCounterRecorder),
		bucketService:   bucketService,
	}
}

func newInternalDRSBucketService(store any) *buckets.Service {
	service, err := buckets.NewService(buckets.Dependencies{
		Credentials:     store.(buckets.CredentialReader),
		CredentialAdmin: store.(buckets.CredentialAdmin),
		Scopes:          store.(buckets.ScopeStore),
		Fallback: func(context.Context) ([]buckets.VisibilityRow, error) {
			return nil, nil
		},
	}, nil)
	if err != nil {
		panic(err)
	}
	return service
}

func (f internalDRSTestFixture) GetObject(ctx context.Context, id, requiredMethod string) (*objects.Record, error) {
	return f.ObjectService.GetObject(ctx, id, requiredMethod)
}

func (f internalDRSTestFixture) RegisterObjects(ctx context.Context, records []objects.Record) error {
	return f.ObjectService.RegisterObjects(ctx, records)
}

func (f internalDRSTestFixture) SaveS3Credential(ctx context.Context, credential *buckets.Credential) error {
	return f.bucketService.SaveS3Credential(ctx, credential)
}

func (f internalDRSTestFixture) CreateBucketScope(ctx context.Context, scope *buckets.Scope) error {
	return f.bucketService.CreateBucketScope(ctx, scope)
}
