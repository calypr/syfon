package objects

import "context"

// OptionalResourceQuery is an optional authorization-aware object ID query.
type OptionalResourceQuery interface {
	ListObjectIDsByResources(ctx context.Context, resources []string, includeUnscoped bool) ([]string, error)
}

// OptionalPageQuery is an optional pagination optimization for object IDs.
type OptionalPageQuery interface {
	ListObjectIDsPageByScope(ctx context.Context, organization, project, startAfter string, limit, offset int) ([]string, error)
	ListObjectIDsPageByResources(ctx context.Context, resources []string, includeUnscoped bool, startAfter string, limit, offset int) ([]string, error)
}

// OptionalURLQuery is an optional URL-filtered pagination optimization.
type OptionalURLQuery interface {
	ListObjectIDsPageByURL(ctx context.Context, objectURL, organization, project, startAfter string, limit, offset int, resources []string, includeUnscoped, restrictToResources bool) ([]string, error)
}

// OptionalAuthorizedQuery is an optional authorization-aware bulk query capability.
type OptionalAuthorizedQuery interface {
	ListObjectIDsByScopeAndResources(ctx context.Context, organization, project string, resources []string, restrictToResources bool) ([]string, error)
	ListObjectIDsByChecksumsAndResources(ctx context.Context, checksums []string, resources []string, includeUnscoped, restrictToResources bool) (map[string][]string, error)
}
