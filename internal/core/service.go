package core

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/urlmanager"
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
	bucketVisibility buckets.VisibilityQuery
	pendingStore     transfers.PendingStore
	transferEvents   transfers.EventRecorder
	fileCounters     usage.FileCounterRecorder
	providerEvents   usage.ProviderEventRecorder
	uM               urlmanager.UrlManager
	bucketService    *buckets.Service
	bucketCatalog    *bucketCatalog
	inspectS3Object  func(context.Context, buckets.Credential, string, string) (*StorageObjectMetadata, error)
	listS3Prefix     func(context.Context, buckets.Credential, string, string, StoragePrefixListOptions) ([]StorageBucketObject, error)
	s3ProbeLimiter   *s3ProbeLimiter
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

// BucketPorts contains the bucket capabilities used by the transitional
// ObjectManager facade. Visibility is an optional optimization.
type BucketPorts struct {
	Credentials     buckets.CredentialReader
	CredentialAdmin buckets.CredentialAdmin
	Scopes          buckets.ScopeStore
	Visibility      buckets.VisibilityQuery
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
	Buckets       BucketPorts
	BucketService *buckets.Service
	Transfers     TransferPorts
	Usage         UsagePorts
}

type VisibleBucket struct {
	Credential buckets.Credential
	Programs   []string
}

func NewObjectManager(deps Dependencies, uM urlmanager.UrlManager) *ObjectManager {
	return &ObjectManager{
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
		bucketVisibility: deps.Buckets.Visibility,
		pendingStore:     deps.Transfers.Pending,
		transferEvents:   deps.Transfers.Events,
		fileCounters:     deps.Usage.Counters,
		providerEvents:   deps.Usage.ProviderEvents,
		uM:               uM,
		bucketService:    deps.BucketService,
		bucketCatalog:    newBucketCatalog(deps.Buckets.Credentials, deps.Buckets.CredentialAdmin, deps.Buckets.Scopes, uM, 30*time.Second),
		inspectS3Object:  defaultS3ObjectInspector,
		listS3Prefix:     defaultS3PrefixLister,
		s3ProbeLimiter:   newS3ProbeLimiterFromEnv(),
	}
}
