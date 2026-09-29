package storage

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/calypr/syfon/internal/buckets"
)

type requestCacheKey struct{}

type requestCache struct {
	mu            sync.Mutex
	credentials   map[string]credentialEntry
	visibleLoaded bool
	visibleValue  map[string]buckets.VisibleBucket
	visibleErr    error
	probes        map[probeRequestKey]*probeEntry
}

type probeRequestKey struct {
	objectURL    string
	organization string
	project      string
	key          string
	scheme       string
}

type probeEntry struct {
	ready    chan struct{}
	metadata *objectMetadata
	err      error
}

type credentialEntry struct {
	credential *buckets.Credential
	err        error
}

func withRequestCache(ctx context.Context) context.Context {
	if cacheFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, requestCacheKey{}, &requestCache{credentials: make(map[string]credentialEntry), probes: make(map[probeRequestKey]*probeEntry)})
}

func cacheFromContext(ctx context.Context) *requestCache {
	cache, _ := ctx.Value(requestCacheKey{}).(*requestCache)
	return cache
}

func cacheCredential(ctx context.Context, bucket string, credential *buckets.Credential, err error) {
	if cache := cacheFromContext(ctx); cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		var copyCredential *buckets.Credential
		if credential != nil {
			copy := *credential
			copyCredential = &copy
		}
		cache.credentials[strings.ToLower(strings.TrimSpace(bucket))] = credentialEntry{credential: copyCredential, err: err}
	}
}

func (cache *requestCache) credential(bucket string) (*buckets.Credential, error, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	entry, ok := cache.credentials[strings.ToLower(strings.TrimSpace(bucket))]
	if !ok {
		return nil, nil, false
	}
	if entry.credential == nil {
		return nil, entry.err, true
	}
	copy := *entry.credential
	return &copy, entry.err, true
}

func cacheVisible(ctx context.Context, visible map[string]buckets.VisibleBucket, err error) {
	if cache := cacheFromContext(ctx); cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		cache.visibleLoaded = true
		cache.visibleValue = cloneVisible(visible)
		cache.visibleErr = err
	}
}

func (cache *requestCache) visible() (map[string]buckets.VisibleBucket, error, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if !cache.visibleLoaded {
		return nil, nil, false
	}
	return cloneVisible(cache.visibleValue), cache.visibleErr, true
}

func cloneVisible(input map[string]buckets.VisibleBucket) map[string]buckets.VisibleBucket {
	if input == nil {
		return nil
	}
	output := make(map[string]buckets.VisibleBucket, len(input))
	for key, value := range input {
		value.Programs = append([]string(nil), value.Programs...)
		output[key] = value
	}
	return output
}

func (cache *requestCache) loadProbe(ctx context.Context, key probeRequestKey, load func() (*objectMetadata, error)) (*objectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cache.mu.Lock()
	if entry, ok := cache.probes[key]; ok {
		cache.mu.Unlock()
		select {
		case <-entry.ready:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return entry.metadata, entry.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	entry := &probeEntry{ready: make(chan struct{})}
	cache.probes[key] = entry
	cache.mu.Unlock()

	entry.metadata, entry.err = load()
	if err := ctx.Err(); err != nil {
		entry.metadata, entry.err = nil, err
	}
	cache.mu.Lock()
	if errors.Is(entry.err, context.Canceled) || errors.Is(entry.err, context.DeadlineExceeded) {
		delete(cache.probes, key)
	}
	close(entry.ready)
	cache.mu.Unlock()
	return entry.metadata, entry.err
}
