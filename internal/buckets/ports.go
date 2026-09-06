package buckets

import "context"

// OptionalVisibilityQuery is an optional object-projection optimization for
// bucket visibility. The bucket service supplies the object-scan fallback.
type OptionalVisibilityQuery interface {
	ListBucketVisibilityRows(ctx context.Context, resources []string, includeUnscoped, restrictToResources bool) ([]VisibilityRow, error)
}
