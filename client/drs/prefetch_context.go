package drs

import "context"

type prefetchedBySHAContextKey struct{}

// WithPrefetchedBySHA stores pre-resolved DRS records keyed by normalized sha256.
func WithPrefetchedBySHA(ctx context.Context, bySHA map[string]DRSObject) context.Context {
	if len(bySHA) == 0 {
		return ctx
	}
	return context.WithValue(ctx, prefetchedBySHAContextKey{}, bySHA)
}

// PrefetchedBySHA returns a pre-resolved DRS record for a normalized sha256.
func PrefetchedBySHA(ctx context.Context, sha256 string) (DRSObject, bool) {
	m, ok := ctx.Value(prefetchedBySHAContextKey{}).(map[string]DRSObject)
	if !ok || len(m) == 0 {
		return DRSObject{}, false
	}
	obj, exists := m[sha256]
	return obj, exists
}

