package objects

import (
	"context"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
)

// LookupResult retains the result for one requested identifier, including a
// missing or denied error that a filtered bulk list cannot represent.
type LookupResult struct {
	Object *drs.DrsObject
	Err    error
}

// GetObjects resolves identifiers in one batch while preserving GetObject's
// per-identifier lookup and authorization rules.
func (s *Service) GetObjects(ctx context.Context, identifiers []string, method string) (map[string]LookupResult, error) {
	result := make(map[string]LookupResult, len(identifiers))
	requested := make([]string, 0, len(identifiers))
	shaQueries := make([]string, 0)
	seenSHA := make(map[string]struct{})
	for _, identifier := range identifiers {
		identifier = strings.TrimSpace(identifier)
		if _, seen := result[identifier]; seen {
			continue
		}
		result[identifier] = LookupResult{Err: errorapi.ErrObjectNotFound}
		if identifier == "" {
			continue
		}
		requested = append(requested, identifier)
		if sha := NormalizeOID(identifier); sha != "" {
			if _, seen := seenSHA[sha]; !seen {
				seenSHA[sha] = struct{}{}
				shaQueries = append(shaQueries, sha)
			}
		}
	}
	if len(requested) == 0 {
		return result, nil
	}

	shaFamilies, err := s.store.GetObjectsByChecksums(ctx, shaQueries)
	if err != nil {
		return nil, err
	}
	physicalQueries := make([]string, 0, len(requested))
	for _, identifier := range requested {
		sha := NormalizeOID(identifier)
		if sha != "" && len(objectsWithSHA256(shaFamilies[sha], sha)) > 0 {
			continue
		}
		physicalQueries = append(physicalQueries, identifier)
	}
	physicalIDs, err := s.store.ResolveObjectIDs(ctx, physicalQueries)
	if err != nil {
		return nil, err
	}
	physical, err := s.store.GetBulkObjects(ctx, distinctValues(physicalIDs))
	if err != nil {
		return nil, err
	}
	physicalByID := make(map[string]drs.DrsObject, len(physical))
	for _, object := range physical {
		physicalByID[object.Id] = object
	}
	genericQueries := make([]string, 0)
	for _, identifier := range physicalQueries {
		_, found := physicalByID[physicalIDs[identifier]]
		if NormalizeOID(identifier) == "" && !found {
			genericQueries = append(genericQueries, identifier)
		}
	}
	genericMatches, err := s.store.GetObjectsByChecksums(ctx, genericQueries)
	if err != nil {
		return nil, err
	}

	all := make([]drs.DrsObject, 0, len(physical)+len(genericMatches))
	all = append(all, physical...)
	for _, family := range shaFamilies {
		all = append(all, family...)
	}
	for _, matches := range genericMatches {
		all = append(all, matches...)
	}
	policy := make(map[string]bool)
	if len(shaFamilies) > 0 || len(genericMatches) > 0 {
		policy, err = s.publicReadPolicy(ctx, all)
		if err != nil {
			return nil, err
		}
	}

	selected := make(map[string]drs.DrsObject, len(requested))
	for _, identifier := range requested {
		if sha := NormalizeOID(identifier); sha != "" {
			family := canonicalizeContentObjects(objectsWithSHA256(shaFamilies[sha], sha), policy)
			if len(family) > 0 {
				if hasObjectMethod(ctx, &family[0], method, policy) {
					result[identifier] = LookupResult{Object: &family[0]}
				} else {
					result[identifier] = LookupResult{Err: errorapi.ErrAccessDenied}
				}
				continue
			}
		}
		if object, found := physicalByID[physicalIDs[identifier]]; found {
			selected[identifier] = object
			continue
		}
		if NormalizeOID(identifier) != "" {
			continue
		}
		matches := canonicalizeContentObjects(genericMatches[identifier], policy)
		readable := filterObjectsByMethod(ctx, matches, method, policy)
		if len(readable) > 0 {
			selected[identifier] = readable[0]
		} else if len(matches) > 0 {
			result[identifier] = LookupResult{Err: errorapi.ErrAccessDenied}
		}
	}
	if len(selected) == 0 {
		return result, nil
	}

	neededSHA := make([]string, 0, len(selected))
	seenSHA = make(map[string]struct{}, len(selected))
	for _, object := range selected {
		if sha, ok := CanonicalSHA256(object.Checksums); ok {
			if _, seen := seenSHA[sha]; !seen {
				seenSHA[sha] = struct{}{}
				neededSHA = append(neededSHA, sha)
			}
		}
	}
	fullFamilies, err := s.store.GetObjectsByChecksums(ctx, neededSHA)
	if err != nil {
		return nil, err
	}
	for _, family := range fullFamilies {
		all = append(all, family...)
	}
	policy, err = s.publicReadPolicy(ctx, all)
	if err != nil {
		return nil, err
	}
	for identifier, object := range selected {
		resolved := cloneObject(object)
		if sha, ok := CanonicalSHA256(object.Checksums); ok {
			family := canonicalizeContentObjects(objectsWithSHA256(fullFamilies[sha], sha), policy)
			if len(family) == 0 {
				continue
			}
			resolved = family[0]
		}
		if !hasObjectMethod(ctx, &resolved, method, policy) {
			result[identifier] = LookupResult{Err: errorapi.ErrAccessDenied}
			continue
		}
		result[identifier] = LookupResult{Object: &resolved}
	}
	return result, nil
}

func distinctValues(values map[string]string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}
