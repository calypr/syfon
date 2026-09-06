package core

import (
	"context"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
)

type contextKey string

var baseURLKey contextKey = "baseURL"

const (
	objectMethodRead   = "read"
	objectMethodCreate = "create"
	objectMethodUpdate = "update"
	objectMethodDelete = "delete"
)

// WithBaseURL adds the base URL to the context.
func WithBaseURL(ctx context.Context, baseURL string) context.Context {
	return context.WithValue(ctx, baseURLKey, baseURL)
}

// GetBaseURL retrieves the base URL from the context.
func GetBaseURL(ctx context.Context) string {
	val, _ := ctx.Value(baseURLKey).(string)
	return val
}

// ObjectManager standardizes object lifecycle operations across all API surfaces.
type ObjectManager struct {
	objectService    *objects.Service
	objectReader     objects.RecordReader
	objectWriter     objects.RecordWriter
	objectAccess     objects.AccessMethodWriter
	objectPolicy     objects.AccessPolicyWriter
	objectAliases    objects.AliasStore
	objectContent    objects.ContentReader
	objectChecksum   objects.ChecksumScopeQuery
	objectScope      objects.ScopeQuery
	objectResources  objects.OptionalResourceQuery
	objectPages      objects.OptionalPageQuery
	objectURLPages   objects.OptionalURLQuery
	objectAuthorized objects.OptionalAuthorizedQuery
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
		objectReader:     deps.Objects.Reader,
		objectWriter:     deps.Objects.Writer,
		objectAccess:     deps.Objects.AccessMethods,
		objectPolicy:     deps.Objects.AccessPolicy,
		objectAliases:    deps.Objects.Aliases,
		objectContent:    deps.Objects.Content,
		objectChecksum:   deps.Objects.ChecksumScope,
		objectScope:      deps.Objects.Scope,
		objectResources:  deps.Objects.Resources,
		objectPages:      deps.Objects.Pages,
		objectURLPages:   deps.Objects.URLPages,
		objectAuthorized: deps.Objects.Authorized,
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
