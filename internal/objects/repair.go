package objects

import (
	"context"
	"fmt"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	clientaccess "github.com/calypr/syfon/client/access"
	"github.com/calypr/syfon/internal/access"
)

type controlledAccessRepairStore interface {
	AddObjectControlledAccess(context.Context, string, string) error
}

// RepairMissingControlledAccess restores a project association only when the
// record ID is derived from its SHA-256 and that exact project scope.
func (s *Service) RepairMissingControlledAccess(ctx context.Context, id, sha string, scope Scope) error {
	scope, err := NewScope(scope.Organization, scope.Project)
	if err != nil {
		return err
	}
	if scope.Organization == "" || scope.Project == "" {
		return fmt.Errorf("%w: project scope is required", errorapi.ErrInvalidInput)
	}
	id = strings.TrimSpace(id)
	sha, valid := CanonicalSHA256([]drs.Checksum{{Type: "sha256", Checksum: sha}})
	if id == "" || !valid {
		return errorapi.ErrNoValidSHA256
	}
	resource, err := clientaccess.ResourcePath(scope.Organization, scope.Project)
	if err != nil {
		return err
	}
	derivedID, err := MintRecordIDFromChecksum(sha, []string{resource})
	if err != nil || derivedID != id {
		return fmt.Errorf("%w: record ID does not match the requested project and SHA-256", errorapi.ErrInvalidInput)
	}
	if access.IsAuthzEnforced(ctx) &&
		!access.HasObjectMethodAccess(ctx, objectMethodUpdate, []string{resource}) &&
		!access.HasMethodAccess(ctx, objectMethodUpdate, []string{"/programs"}) &&
		!access.HasMethodAccess(ctx, objectMethodUpdate, []string{"/data_file"}) {
		return errorapi.ErrAccessDenied
	}
	record, err := s.store.GetObject(ctx, id)
	if err != nil {
		return err
	}
	if record == nil || record.Id != id {
		return errorapi.ErrObjectNotFound
	}
	storedSHA, hasSHA := CanonicalSHA256(record.Checksums)
	if !hasSHA || storedSHA != sha {
		return fmt.Errorf("%w: stored SHA-256 does not match repair request", errorapi.ErrInvalidInput)
	}
	for _, existing := range AccessResources(record) {
		if existing == resource {
			return nil
		}
	}
	store, ok := s.store.(controlledAccessRepairStore)
	if !ok {
		return fmt.Errorf("controlled-access repair persistence is not configured")
	}
	return store.AddObjectControlledAccess(ctx, id, resource)
}
