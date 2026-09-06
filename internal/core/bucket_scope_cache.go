package core

import (
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/internal/buckets"
)

type bucketScopeCache struct {
	mu      sync.RWMutex
	ttl     time.Duration
	entries map[string]cachedBucketScope
}

type cachedBucketScope struct {
	scope   buckets.Scope
	found   bool
	expires time.Time
}

func newBucketScopeCache(ttl time.Duration) *bucketScopeCache {
	return &bucketScopeCache{
		ttl:     ttl,
		entries: make(map[string]cachedBucketScope),
	}
}

func (c *bucketScopeCache) get(organization, project string) (buckets.Scope, bool, bool) {
	if c == nil {
		return buckets.Scope{}, false, false
	}
	key := bucketScopeCacheKey(organization, project)
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || now.After(entry.expires) {
		return buckets.Scope{}, false, false
	}
	return entry.scope, entry.found, true
}

func (c *bucketScopeCache) set(scope buckets.Scope, found bool) {
	if c == nil {
		return
	}
	scope = normalizeBucketScope(&scope)
	c.mu.Lock()
	c.entries[bucketScopeCacheKey(scope.Organization, scope.ProjectID)] = cachedBucketScope{
		scope:   scope,
		found:   found,
		expires: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

func (c *bucketScopeCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.entries = make(map[string]cachedBucketScope)
	c.mu.Unlock()
}

func bucketScopeCacheKey(organization, project string) string {
	return strings.TrimSpace(organization) + "\x00" + strings.TrimSpace(project)
}

func normalizeBucketScope(scope *buckets.Scope) buckets.Scope {
	if scope == nil {
		return buckets.Scope{}
	}
	return buckets.Scope{
		Organization: strings.TrimSpace(scope.Organization),
		ProjectID:    strings.TrimSpace(scope.ProjectID),
		CredentialID: strings.TrimSpace(scope.CredentialID),
		Bucket:       strings.TrimSpace(scope.Bucket),
		PathPrefix:   strings.Trim(strings.TrimSpace(scope.PathPrefix), "/"),
	}
}
