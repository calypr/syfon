package buckets

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/access"
	"github.com/calypr/syfon/internal/faults"
	"github.com/calypr/syfon/internal/storage/address"
)

const (
	defaultScopeCacheTTL = 30 * time.Second
	readMethod           = "read"
)

var errMissingVisibilitySource = errors.New("bucket service requires a visibility query or fallback")

// Dependencies are the narrow repository and visibility ports used by Service.
// Visibility is an optional persistence optimization; Fallback is the
// composition-owned object scan used when that optimization is unavailable.
type Dependencies struct {
	Credentials     CredentialReader
	CredentialAdmin CredentialAdmin
	Scopes          ScopeStore
	Visibility      VisibilityQuery
	Fallback        VisibilityFallback
}

// cacheInvalidator is deliberately private. A composition-layer concrete
// manager can satisfy this one-method seam without making buckets depend on
// storage, URL signing, or another parent package.
type cacheInvalidator interface {
	InvalidateBucket(string)
}

// Service owns bucket credential and scope policy, including scope-cache and
// signer-cache invalidation rules. Repository adapters remain responsible for
// SQL, encryption, auditing, and transaction semantics.
type Service struct {
	credentialReader       CredentialReader
	credentialAdmin        CredentialAdmin
	scopeStore             ScopeStore
	visibility             VisibilityQuery
	fallback               VisibilityFallback
	scopeCache             *scopeCache
	signerCacheInvalidator cacheInvalidator
}

// NewService validates and constructs the bucket service with its production
// scope-cache lifetime. A nil invalidator is a supported no-op configuration.
func NewService(deps Dependencies, invalidator cacheInvalidator) (*Service, error) {
	if deps.Credentials == nil {
		return nil, errors.New("bucket service requires credential reader")
	}
	if deps.CredentialAdmin == nil {
		return nil, errors.New("bucket service requires credential admin")
	}
	if deps.Scopes == nil {
		return nil, errors.New("bucket service requires scope store")
	}
	if deps.Visibility == nil && deps.Fallback == nil {
		return nil, errMissingVisibilitySource
	}
	return newService(deps, invalidator, defaultScopeCacheTTL, time.Now), nil
}

// newService keeps clock and TTL injection private to this package's tests.
// Production callers use NewService and cannot accidentally create a cache
// with a nonstandard lifetime.
func newService(deps Dependencies, invalidator cacheInvalidator, ttl time.Duration, now func() time.Time) *Service {
	return &Service{
		credentialReader:       deps.Credentials,
		credentialAdmin:        deps.CredentialAdmin,
		scopeStore:             deps.Scopes,
		visibility:             deps.Visibility,
		fallback:               deps.Fallback,
		scopeCache:             newScopeCache(ttl, now),
		signerCacheInvalidator: invalidator,
	}
}

// ListS3Credentials returns configured credentials in repository order. The
// first returned credential remains the default bucket selection.
func (s *Service) ListS3Credentials(ctx context.Context) ([]Credential, error) {
	return s.credentialReader.ListS3Credentials(ctx)
}

// GetS3Credential resolves a credential by its canonical ID or legacy physical
// bucket alias according to the repository's compatibility contract.
func (s *Service) GetS3Credential(ctx context.Context, bucket string) (*Credential, error) {
	return s.credentialReader.GetS3Credential(ctx, bucket)
}

// SaveS3Credential persists a credential before invalidating every identity
// alias that could key a provider signer.
func (s *Service) SaveS3Credential(ctx context.Context, cred *Credential) error {
	requestedID := ""
	physicalBucket := ""
	if cred != nil {
		requestedID = s.credentialIDForCredential(*cred)
		physicalBucket = strings.TrimSpace(cred.Bucket)
	}
	if err := s.credentialAdmin.SaveS3Credential(ctx, cred); err != nil {
		return err
	}

	aliases := []string{requestedID, physicalBucket}
	if cred != nil {
		aliases = append(aliases, s.credentialIDForCredential(*cred), cred.CredentialID, cred.Bucket)
	}
	s.invalidateAliases(aliases...)
	return nil
}

// DeleteS3Credential resolves aliases before mutation, then invalidates all
// known aliases only after the repository confirms deletion. A failed
// pre-resolution does not mask the repository delete result.
func (s *Service) DeleteS3Credential(ctx context.Context, bucket string) error {
	requested := strings.TrimSpace(bucket)
	var resolved *Credential
	if cred, err := s.credentialReader.GetS3Credential(ctx, bucket); err == nil && cred != nil {
		copy := *cred
		resolved = &copy
	}

	if err := s.credentialAdmin.DeleteS3Credential(ctx, bucket); err != nil {
		return err
	}
	s.scopeCache.clear()
	aliases := []string{requested}
	if resolved != nil {
		aliases = append(aliases, resolved.CredentialID, resolved.Bucket)
	}
	s.invalidateAliases(aliases...)
	return nil
}

// ListBucketScopes delegates scope enumeration without changing repository
// order or adapter-owned normalization.
func (s *Service) ListBucketScopes(ctx context.Context) ([]Scope, error) {
	return s.scopeStore.ListBucketScopes(ctx)
}

// CreateBucketScope persists a scope and only then seeds the positive cache.
func (s *Service) CreateBucketScope(ctx context.Context, scope *Scope) error {
	if err := s.scopeStore.CreateBucketScope(ctx, scope); err != nil {
		return err
	}
	s.scopeCache.set(normalizeScope(scope), true)
	return nil
}

// DeleteBucketScope deletes the requested scope, clears all cached scope
// answers, and preserves the existing last-scope credential cleanup policy.
func (s *Service) DeleteBucketScope(ctx context.Context, organization, projectID, credentialID, pathPrefix string) error {
	if err := s.scopeStore.DeleteBucketScope(ctx, organization, projectID, credentialID, pathPrefix); err != nil {
		return err
	}
	s.scopeCache.clear()

	resolvedID := strings.TrimSpace(credentialID)
	resolvedBucket := ""
	if cred, err := s.GetS3Credential(ctx, credentialID); err == nil && cred != nil {
		resolvedID = s.credentialIDForCredential(*cred)
		resolvedBucket = strings.TrimSpace(cred.Bucket)
	}

	scopes, err := s.ListBucketScopes(ctx)
	if err != nil {
		return nil
	}
	for _, scope := range scopes {
		if s.scopeBelongsTo(scope, resolvedID, resolvedBucket) {
			return nil
		}
	}
	// This cleanup is intentionally best-effort, matching the former catalog:
	// the requested scope was already deleted successfully and an inability to
	// remove an orphan credential must not turn that success into an error.
	_ = s.DeleteS3Credential(ctx, resolvedID)
	return nil
}

// LookupBucketScope returns a normalized scope and caches both hits and
// not-found misses. Backend errors other than not-found are never cached.
func (s *Service) LookupBucketScope(ctx context.Context, organization, project string) (Scope, bool, error) {
	if scope, found, cached := s.scopeCache.get(organization, project); cached {
		return scope, found, nil
	}

	scope, err := s.scopeStore.GetBucketScope(ctx, organization, project)
	if err != nil {
		if faults.IsNotFoundError(err) {
			s.scopeCache.set(Scope{Organization: organization, ProjectID: project}, false)
			return Scope{}, false, nil
		}
		return Scope{}, false, err
	}
	if scope == nil {
		s.scopeCache.set(Scope{Organization: organization, ProjectID: project}, false)
		return Scope{}, false, nil
	}

	normalized := normalizeScope(scope)
	s.scopeCache.set(normalized, true)
	return normalized, true, nil
}

// ResolveBucket selects the first repository-returned credential for an empty
// request, or validates an explicitly requested physical bucket.
func (s *Service) ResolveBucket(ctx context.Context, bucketName string) (string, error) {
	creds, err := s.ListS3Credentials(ctx)
	if err != nil {
		return "", err
	}
	if len(creds) == 0 {
		return "", fmt.Errorf("no buckets configured")
	}
	if bucketName == "" {
		return creds[0].Bucket, nil
	}
	for _, cred := range creds {
		if cred.Bucket == bucketName {
			return cred.Bucket, nil
		}
	}
	return "", fmt.Errorf("bucket %q not configured", bucketName)
}

// ListVisibleBuckets assembles configured credentials, explicit scopes, and
// object-derived rows. The optional query wins; the fallback is used only when
// that optimization is absent.
func (s *Service) ListVisibleBuckets(ctx context.Context) (map[string]VisibleBucket, error) {
	creds, err := s.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	if len(creds) == 0 {
		return map[string]VisibleBucket{}, nil
	}

	var rows []VisibilityRow
	if s.visibility != nil {
		restrictToResources := access.IsAuthzEnforced(ctx) &&
			!access.HasMethodAccess(ctx, readMethod, []string{"/programs"}) &&
			!access.HasMethodAccess(ctx, readMethod, []string{"/data_file"})
		rows, err = s.visibility.ListBucketVisibilityRows(ctx, readableResources(ctx), true, restrictToResources)
	} else {
		// NewService rejects this state. Keep the guard for package-local
		// zero-value construction and future composition mistakes.
		if s.fallback == nil {
			return nil, errMissingVisibilitySource
		}
		rows, err = s.fallback(ctx)
	}
	if err != nil {
		return nil, err
	}

	return s.mergeVisibleRows(ctx, creds, rows)
}

func (s *Service) mergeVisibleRows(ctx context.Context, creds []Credential, rows []VisibilityRow) (map[string]VisibleBucket, error) {
	byCredential := make(map[string]VisibleBucket, len(creds))
	programsSeen := make(map[string]map[string]struct{}, len(creds))
	for _, cred := range creds {
		key := s.credentialIDForCredential(cred)
		byCredential[key] = VisibleBucket{Credential: cred}
		programsSeen[key] = map[string]struct{}{}
	}

	scopes, err := s.ListBucketScopes(ctx)
	if err != nil {
		return nil, err
	}
	explicitScopeOwners := make(map[string]string, len(scopes))
	for _, scope := range scopes {
		credentialID := s.scopeCredentialIDForCredentials(scope, creds)
		entry, exists := byCredential[credentialID]
		if !exists {
			continue
		}
		resource, resourceErr := resourcePath(scope.Organization, scope.ProjectID)
		if resourceErr != nil || strings.TrimSpace(resource) == "" {
			continue
		}
		if access.IsAuthzEnforced(ctx) && !access.HasMethodAccess(ctx, readMethod, []string{resource}) {
			continue
		}
		if _, seen := programsSeen[credentialID][resource]; seen {
			continue
		}
		explicitScopeOwners[resource] = credentialID
		programsSeen[credentialID][resource] = struct{}{}
		entry.Programs = append(entry.Programs, resource)
		byCredential[credentialID] = entry
	}

	for _, row := range rows {
		credentialID, ok := s.credentialIDForVisibilityRow(row, creds)
		if !ok {
			continue
		}
		entry, exists := byCredential[credentialID]
		if !exists {
			continue
		}
		resource := strings.TrimSpace(row.Resource)
		if resource == "" {
			// Unscoped rows establish that a credential has public content, but
			// the legacy map projection intentionally has no separate flag.
			continue
		}
		if owner, ok := explicitScopeOwners[resource]; ok && owner != credentialID {
			continue
		}
		if _, seen := programsSeen[credentialID][resource]; seen {
			continue
		}
		programsSeen[credentialID][resource] = struct{}{}
		entry.Programs = append(entry.Programs, resource)
		byCredential[credentialID] = entry
	}

	for credentialID, entry := range byCredential {
		sort.Strings(entry.Programs)
		byCredential[credentialID] = entry
	}
	return byCredential, nil
}

// VisibleToCaller reports whether a caller's bucket or credential aliases are
// represented in a visible bucket map. Matching is case-insensitive for
// compatibility with legacy physical IDs.
func VisibleToCaller(visible map[string]VisibleBucket, bucket, credentialID string) bool {
	for key, entry := range visible {
		if strings.EqualFold(strings.TrimSpace(entry.Credential.Bucket), bucket) ||
			strings.EqualFold(strings.TrimSpace(key), credentialID) ||
			strings.EqualFold(strings.TrimSpace(entry.Credential.CredentialID), credentialID) {
			return true
		}
	}
	return false
}

func (s *Service) invalidateAliases(aliases ...string) {
	if s.signerCacheInvalidator == nil {
		return
	}
	seen := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		if _, ok := seen[alias]; ok {
			continue
		}
		seen[alias] = struct{}{}
		s.signerCacheInvalidator.InvalidateBucket(alias)
	}
}

func (s *Service) credentialIDForCredential(cred Credential) string {
	if credentialID := strings.TrimSpace(cred.CredentialID); credentialID != "" {
		return credentialID
	}
	return strings.TrimSpace(cred.Bucket)
}

func (s *Service) credentialIDForScope(scope Scope) string {
	if credentialID := strings.TrimSpace(scope.CredentialID); credentialID != "" {
		return credentialID
	}
	return strings.TrimSpace(scope.Bucket)
}

func (s *Service) scopeCredentialIDForCredentials(scope Scope, creds []Credential) string {
	candidate := s.credentialIDForScope(scope)
	for _, cred := range creds {
		if strings.EqualFold(candidate, s.credentialIDForCredential(cred)) ||
			strings.EqualFold(candidate, strings.TrimSpace(cred.Bucket)) ||
			strings.EqualFold(candidate, strings.TrimSpace(cred.CredentialID)) {
			return s.credentialIDForCredential(cred)
		}
	}
	return candidate
}

func (s *Service) scopeBelongsTo(scope Scope, credentialID, bucket string) bool {
	for _, candidate := range []string{scope.CredentialID, scope.Bucket} {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(credentialID)) {
			return true
		}
		if bucket != "" && strings.EqualFold(strings.TrimSpace(candidate), bucket) {
			return true
		}
	}
	return false
}

func (s *Service) credentialIDForVisibilityRow(row VisibilityRow, creds []Credential) (string, bool) {
	bucket, ok := bucketForVisibilityRow(row, creds)
	if !ok {
		return "", false
	}
	for _, cred := range creds {
		if strings.TrimSpace(cred.Bucket) == bucket {
			return s.credentialIDForCredential(cred), true
		}
	}
	return bucket, true
}

func bucketForVisibilityRow(row VisibilityRow, creds []Credential) (string, bool) {
	raw := strings.TrimSpace(row.AccessURL)
	if raw == "" {
		return "", false
	}
	if bucket, _, ok := address.ParseS3URL(raw); ok {
		return bucket, true
	}
	scheme := address.SchemeFromURL(raw)
	if provider := address.ProviderFromScheme(scheme); provider == address.GCSProvider || provider == address.AzureProvider {
		if parsed, err := url.Parse(raw); err == nil && strings.TrimSpace(parsed.Host) != "" {
			return strings.TrimSpace(parsed.Host), true
		}
	}

	cleanRaw := filepath.Clean(raw)
	for _, cred := range creds {
		if address.NormalizeProvider(cred.Provider, address.S3Provider) != address.FileProvider {
			continue
		}
		root := strings.TrimSpace(cred.Endpoint)
		if root == "" {
			root = strings.TrimSpace(cred.Bucket)
		}
		if root == "" {
			continue
		}
		cleanRoot := filepath.Clean(root)
		if cleanRaw == cleanRoot || strings.HasPrefix(cleanRaw, cleanRoot+string(filepath.Separator)) {
			return cred.Bucket, true
		}
	}
	return "", false
}

func resourcePath(org, project string) (string, error) {
	org = strings.TrimSpace(org)
	project = strings.TrimSpace(project)
	if org == "" && project == "" {
		return "", nil
	}
	if org == "" {
		return "", fmt.Errorf("organization required when project is specified")
	}
	if project == "" {
		return "/organization/" + org, nil
	}
	return "/organization/" + org + "/project/" + project, nil
}

func readableResources(ctx context.Context) []string {
	return authorizedResources(ctx, readMethod)
}

func authorizedResources(ctx context.Context, method string) []string {
	privileges := access.GetUserPrivileges(ctx)
	if len(privileges) == 0 {
		return normalizeAccessResources(access.GetUserAuthz(ctx))
	}
	resources := make([]string, 0, len(privileges))
	for resource, methods := range privileges {
		if methods[method] || methods["*"] {
			resources = append(resources, resource)
		}
	}
	return normalizeAccessResources(resources)
}

func normalizeAccessResources(resources []string) []string {
	seen := make(map[string]struct{}, len(resources))
	result := make([]string, 0, len(resources))
	for _, raw := range resources {
		resource := normalizeAccessResource(raw)
		if resource == "" {
			continue
		}
		if _, ok := seen[resource]; ok {
			continue
		}
		seen[resource] = struct{}{}
		result = append(result, resource)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func normalizeAccessResource(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	path := raw
	if parsed, err := url.Parse(raw); err == nil && parsed.Path != "" {
		path = parsed.Path
	}
	path = "/" + strings.Trim(path, "/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return raw
	}
	if parts[0] != "program" && parts[0] != "programs" && parts[0] != "organization" && parts[0] != "organizations" {
		return raw
	}
	org := strings.TrimSpace(parts[1])
	if org == "" {
		return ""
	}
	if len(parts) >= 4 && (parts[2] == "project" || parts[2] == "projects") {
		project := strings.TrimSpace(parts[3])
		if project != "" {
			return "/organization/" + org + "/project/" + project
		}
	}
	return "/organization/" + org
}
