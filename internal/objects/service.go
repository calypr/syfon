package objects

const (
	objectMethodRead   = "read"
	objectMethodCreate = "create"
	objectMethodUpdate = "update"
	objectMethodDelete = "delete"
)

// Service owns object registration, reads, canonicalization, authorization,
// aliases, and object-level mutations. Persistence adapters provide the
// narrow ports in Dependencies; the service does not know about storage,
// buckets, transfers, HTTP, or SQL.
type Service struct {
	recordReader    RecordReader
	recordWriter    RecordWriter
	accessMethods   AccessMethodWriter
	accessPolicy    AccessPolicyWriter
	aliases         AliasStore
	content         ContentReader
	checksumScope   ChecksumScopeQuery
	scope           ScopeQuery
	resources       OptionalResourceQuery
	pages           OptionalPageQuery
	urlPages        OptionalURLQuery
	authorizedQuery OptionalAuthorizedQuery
}

// Dependencies contains the object-owned ports used by Service. The query
// ports are optional optimizations; the service retains scan-based behavior
// when they are unavailable.
type Dependencies struct {
	Reader        RecordReader
	Writer        RecordWriter
	AccessMethods AccessMethodWriter
	AccessPolicy  AccessPolicyWriter
	Aliases       AliasStore
	Content       ContentReader
	ChecksumScope ChecksumScopeQuery
	Scope         ScopeQuery
	Resources     OptionalResourceQuery
	Pages         OptionalPageQuery
	URLPages      OptionalURLQuery
	Authorized    OptionalAuthorizedQuery
}

// NewService composes the object service from its independent object ports.
// Required-port validation remains at the composition boundary so lightweight
// tests and transitional callers can still construct partial services.
func NewService(deps Dependencies) *Service {
	return &Service{
		recordReader:    deps.Reader,
		recordWriter:    deps.Writer,
		accessMethods:   deps.AccessMethods,
		accessPolicy:    deps.AccessPolicy,
		aliases:         deps.Aliases,
		content:         deps.Content,
		checksumScope:   deps.ChecksumScope,
		scope:           deps.Scope,
		resources:       deps.Resources,
		pages:           deps.Pages,
		urlPages:        deps.URLPages,
		authorizedQuery: deps.Authorized,
	}
}
