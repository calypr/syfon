package objects

import "context"

// CanonicalizeContentObjects exposes the deterministic same-checksum merge to
// transitional package-local tests while keeping its implementation owned by
// the object service package.
func CanonicalizeContentObjects(records []Record) []Record {
	return canonicalizeContentObjects(records)
}

// SearchAfterID returns the first sorted ID strictly greater than startAfter.
func SearchAfterID(ids []string, startAfter string) int {
	return searchAfterID(ids, startAfter)
}

// ObjectMatchesScope reports whether a record belongs to an organization and
// optional project scope.
func ObjectMatchesScope(record *Record, organization, project string) bool {
	return objectMatchesScope(record, organization, project)
}

// ReadableChecksumFilter exposes the authorization-derived checksum query
// shape to the transitional core facade.
func (s *Service) ReadableChecksumFilter(ctx context.Context, organization, project string) ([]string, bool, bool, bool) {
	return s.readableChecksumFilter(ctx, organization, project)
}
