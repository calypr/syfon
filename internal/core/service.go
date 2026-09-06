package core

import (
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

const (
	objectMethodRead   = "read"
	objectMethodCreate = "create"
	objectMethodUpdate = "update"
	objectMethodDelete = "delete"
)

// ObjectManager standardizes object lifecycle operations across all API surfaces.
type ObjectManager struct {
	objectService    *objects.Service
	pendingStore     transfers.PendingStore
	transferEvents   transfers.EventRecorder
	fileCounters     usage.FileCounterRecorder
	providerEvents   usage.ProviderEventRecorder
	storageAccess    StorageAccess
	storageMultipart StorageMultipart
	storageProbe     StorageProbe
	storageInventory StorageInventory
	storageDelete    StorageDelete
	bucketService    *buckets.Service
}

// ObjectPorts contains the object capabilities used by the transitional
// ObjectManager facade. Optional query ports may be nil.
type ObjectPorts struct {
	Reader        objects.RecordReader
	Writer        objects.RecordWriter
	AccessMethods objects.AccessMethodWriter
	AccessPolicy  objects.AccessPolicyWriter
	Aliases       objects.AliasStore
	Content       objects.ContentReader
	ChecksumScope objects.ChecksumScopeQuery
	Scope         objects.ScopeQuery
	Resources     objects.OptionalResourceQuery
	Pages         objects.OptionalPageQuery
	URLPages      objects.OptionalURLQuery
	Authorized    objects.OptionalAuthorizedQuery
}

// TransferPorts contains pending metadata and transfer-event capabilities.
type TransferPorts struct {
	Pending transfers.PendingStore
	Events  transfers.EventRecorder
}

// UsagePorts contains the accounting capabilities used by the facade.
type UsagePorts struct {
	Counters       usage.FileCounterRecorder
	ProviderEvents usage.ProviderEventRecorder
}

// Dependencies is a concrete composition record, not a replacement database
// interface. Each field is owned by the package that defines its port.
type Dependencies struct {
	Objects       ObjectPorts
	BucketService *buckets.Service
	Transfers     TransferPorts
	Usage         UsagePorts
	Storage       StoragePorts
}

func NewObjectManager(deps Dependencies) *ObjectManager {
	return &ObjectManager{
		objectService: objects.NewService(objects.Dependencies{
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
		}),
		pendingStore:     deps.Transfers.Pending,
		transferEvents:   deps.Transfers.Events,
		fileCounters:     deps.Usage.Counters,
		providerEvents:   deps.Usage.ProviderEvents,
		storageAccess:    deps.Storage.Access,
		storageMultipart: deps.Storage.Multipart,
		storageProbe:     deps.Storage.Probe,
		storageInventory: deps.Storage.Inventory,
		storageDelete:    deps.Storage.Delete,
		bucketService:    deps.BucketService,
	}
}
