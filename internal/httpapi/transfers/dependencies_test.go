package transfers

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/maintenance/projectstorage"
	"github.com/calypr/syfon/internal/maintenance/scoperepair"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

type internalDRSTestFixture struct {
	*core.ObjectManager
	ObjectService      *objects.Service
	TransferService    *transfers.Service
	FileCounters       usage.FileCounterRecorder
	bucketService      *buckets.Service
	projectService     *projectstorage.Service
	scopeRepairService *scoperepair.Service
}

type internalDRSStorageCapabilities struct {
	Probe     projectstorage.ProbePort
	Inventory projectstorage.InventoryPort
	Delete    projectstorage.DeletePort
}

func newInternalDRSObjectManager(store any, storageDependency any) internalDRSTestFixture {
	objectPorts := core.ObjectPorts{
		Reader:        store.(objects.RecordReader),
		Writer:        store.(objects.RecordWriter),
		AccessMethods: store.(objects.AccessMethodWriter),
		AccessPolicy:  store.(objects.AccessPolicyWriter),
		Aliases:       store.(objects.AliasStore),
		Content:       store.(objects.ContentReader),
		ChecksumScope: store.(objects.ChecksumScopeQuery),
		Scope:         store.(objects.ScopeQuery),
		Resources:     optionalInternalDRSPort[objects.OptionalResourceQuery](store),
		Pages:         optionalInternalDRSPort[objects.OptionalPageQuery](store),
		URLPages:      optionalInternalDRSPort[objects.OptionalURLQuery](store),
		Authorized:    optionalInternalDRSPort[objects.OptionalAuthorizedQuery](store),
	}
	bucketService := newInternalDRSBucketService(store, storageDependency, objectPorts)
	storagePorts := internalDRSStoragePorts(storageDependency)
	storageAccess, _ := storageDependency.(transfers.AccessPort)
	storageMultipart, _ := storageDependency.(transfers.MultipartPort)
	objectService := objects.NewService(objects.Dependencies{
		Reader:        objectPorts.Reader,
		Writer:        objectPorts.Writer,
		AccessMethods: objectPorts.AccessMethods,
		AccessPolicy:  objectPorts.AccessPolicy,
		Aliases:       objectPorts.Aliases,
		Content:       objectPorts.Content,
		ChecksumScope: objectPorts.ChecksumScope,
		Scope:         objectPorts.Scope,
		Resources:     objectPorts.Resources,
		Pages:         objectPorts.Pages,
		URLPages:      objectPorts.URLPages,
		Authorized:    objectPorts.Authorized,
	})
	transferService := transfers.NewService(transfers.Dependencies{
		Access:      storageAccess,
		Multipart:   storageMultipart,
		Scopes:      bucketService,
		Credentials: bucketService,
		Pending:     store.(transfers.PendingStore),
		Events:      store.(transfers.EventRecorder),
	})
	return internalDRSTestFixture{
		ObjectManager: core.NewObjectManager(core.Dependencies{
			Objects:       objectPorts,
			BucketService: bucketService,
		}),
		ObjectService:   objectService,
		TransferService: transferService,
		FileCounters:    store.(usage.FileCounterRecorder),
		bucketService:   bucketService,
		projectService: projectstorage.NewService(
			projectstorage.Dependencies{
				Scopes:         bucketService,
				Credentials:    bucketService,
				Visibility:     bucketService,
				Inventory:      storagePorts.Inventory,
				Probe:          storagePorts.Probe,
				Delete:         storagePorts.Delete,
				Physical:       objectService,
				CleanupObjects: objectService,
				CleanupScopes:  bucketService,
			},
		),
		scopeRepairService: nil,
	}
}

func internalDRSStoragePorts(dependency any) internalDRSStorageCapabilities {
	if ports, ok := dependency.(internalDRSStorageCapabilities); ok {
		return ports
	}
	ports := internalDRSStorageCapabilities{}
	ports.Probe, _ = dependency.(projectstorage.ProbePort)
	ports.Inventory, _ = dependency.(projectstorage.InventoryPort)
	ports.Delete, _ = dependency.(projectstorage.DeletePort)
	return ports
}

func newInternalDRSBucketService(store any, storageDependency any, objectPorts core.ObjectPorts) *buckets.Service {
	var invalidator interface{ InvalidateBucket(string) }
	if candidate, ok := storageDependency.(interface{ InvalidateBucket(string) }); ok {
		invalidator = candidate
	}
	service, err := buckets.NewService(buckets.Dependencies{
		Credentials:     store.(buckets.CredentialReader),
		CredentialAdmin: store.(buckets.CredentialAdmin),
		Scopes:          store.(buckets.ScopeStore),
		Visibility:      optionalInternalDRSPort[buckets.VisibilityQuery](store),
		Fallback:        core.NewBucketVisibilityFallback(objectPorts.Scope, objectPorts.Reader),
	}, invalidator)
	if err != nil {
		panic(err)
	}
	return service
}

func optionalInternalDRSPort[T any](store any) T {
	value, _ := store.(T)
	return value
}

var _ transfers.AccessPort = (*internalDRSStorageFake)(nil)
var _ transfers.MultipartPort = (*internalDRSStorageFake)(nil)
var _ projectstorage.ProbePort = (*internalDRSStorageFake)(nil)
var _ projectstorage.InventoryPort = (*internalDRSStorageFake)(nil)
var _ projectstorage.DeletePort = (*internalDRSStorageFake)(nil)
var _ projectstorage.ProbePort = (*internalDRSProbeFake)(nil)
var _ projectstorage.InventoryPort = (*internalDRSInventoryFake)(nil)
var _ projectstorage.DeletePort = (*internalDRSDeleteFake)(nil)
