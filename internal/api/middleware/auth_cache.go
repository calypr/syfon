package middleware

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"time"
)

func newAuthzCache(cfg authCacheConfig) *authzCache {
	return &authzCache{
		cfg:     cfg,
		entries: make(map[string]authzCacheEntry, cfg.MaxEntries),
	}
}

func (c *authzCache) get(key string) ([]string, map[string]map[string]bool, bool, bool) {
	now := time.Now()
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, nil, false, false
	}
	if now.After(entry.expiresAt) {
		c.mu.Lock()
		// Re-check under write lock to avoid deleting refreshed entries.
		if curr, ok := c.entries[key]; ok && now.After(curr.expiresAt) {
			delete(c.entries, key)
		}
		c.mu.Unlock()
		return nil, nil, false, false
	}
	return cloneStrings(entry.resources), clonePrivMap(entry.privileges), entry.negative, true
}

func (c *authzCache) set(key string, resources []string, privileges map[string]map[string]bool, negative bool) {
	ttl := c.cfg.TTL
	if negative {
		ttl = c.cfg.NegativeTTL
	}
	if ttl <= 0 {
		return
	}
	entry := authzCacheEntry{
		resources:  cloneStrings(resources),
		privileges: clonePrivMap(privileges),
		negative:   negative,
		expiresAt:  time.Now().Add(ttl),
	}

	c.mu.Lock()
	c.entries[key] = entry
	if len(c.entries) > c.cfg.MaxEntries {
		c.evictExpiredOrOldestLocked()
	}
	c.mu.Unlock()
}

func (c *authzCache) evictExpiredOrOldestLocked() {
	now := time.Now()
	for k, v := range c.entries {
		if now.After(v.expiresAt) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) <= c.cfg.MaxEntries {
		return
	}

	type kv struct {
		key string
		exp time.Time
	}
	all := make([]kv, 0, len(c.entries))
	for k, v := range c.entries {
		all = append(all, kv{key: k, exp: v.expiresAt})
	}
	sort.Slice(all, func(i, j int) bool {
		return all[i].exp.Before(all[j].exp)
	})

	toDrop := len(c.entries) - c.cfg.MaxEntries
	for i := 0; i < toDrop && i < len(all); i++ {
		delete(c.entries, all[i].key)
	}
}

func tokenCacheKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func clonePrivMap(in map[string]map[string]bool) map[string]map[string]bool {
	if len(in) == 0 {
		return map[string]map[string]bool{}
	}
	out := make(map[string]map[string]bool, len(in))
	for k, methods := range in {
		if methods == nil {
			out[k] = map[string]bool{}
			continue
		}
		mm := make(map[string]bool, len(methods))
		for mk, mv := range methods {
			mm[mk] = mv
		}
		out[k] = mm
	}
	return out
}
