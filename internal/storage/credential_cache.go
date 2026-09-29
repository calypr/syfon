package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/calypr/syfon/internal/buckets"
)

type credentialCacheContextKey struct{}

type credentialCacheKey struct {
	manager   *Manager
	candidate string
}

type credentialLookupResult struct {
	credential *buckets.Credential
	err        error
}

type credentialCache struct {
	mu      sync.Mutex
	entries map[credentialCacheKey]credentialLookupResult
}

// WithCredentialCache enables credential lookup reuse for the lifetime of this
// operation context. Each call creates a fresh cache, so a later operation
// observes credential updates.
func WithCredentialCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, credentialCacheContextKey{}, &credentialCache{
		entries: make(map[credentialCacheKey]credentialLookupResult),
	})
}

// withCredentialCacheIfAbsent preserves an enclosing operation's cache while
// giving standalone operations a fresh one.
func withCredentialCacheIfAbsent(ctx context.Context) context.Context {
	if _, ok := ctx.Value(credentialCacheContextKey{}).(*credentialCache); ok {
		return ctx
	}
	return WithCredentialCache(ctx)
}

func (m *Manager) getCredential(ctx context.Context, candidate string) (*buckets.Credential, error) {
	cache, ok := ctx.Value(credentialCacheContextKey{}).(*credentialCache)
	if !ok {
		return m.credentials.GetS3Credential(ctx, candidate)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := credentialCacheKey{manager: m, candidate: candidate}
	cache.mu.Lock()
	cached, found := cache.entries[key]
	cache.mu.Unlock()
	if found {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return cached.credential, cached.err
	}

	credential, err := m.credentials.GetS3Credential(ctx, candidate)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return credential, err
	}

	cache.mu.Lock()
	if contextErr := ctx.Err(); contextErr != nil {
		cache.mu.Unlock()
		return nil, contextErr
	}
	if previous, exists := cache.entries[key]; exists {
		cached = previous
	} else {
		cached = credentialLookupResult{credential: credential, err: err}
		cache.entries[key] = cached
	}
	cache.mu.Unlock()
	return cached.credential, cached.err
}
