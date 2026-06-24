package core

import (
	"context"
	"fmt"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
)

func (m *ObjectManager) ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error) {
	store, ok := m.db.(db.StorageMetricsStore)
	if !ok {
		return nil, fmt.Errorf("storage cleanup listing not supported by database")
	}
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" {
		return nil, fmt.Errorf("organization and project are required")
	}
	if authz.IsAuthzEnforced(ctx) {
		if authz.IsGen3Mode(ctx) && !authz.HasAuthHeader(ctx) {
			return nil, common.ErrUnauthorized
		}
		resource, err := sycommon.ResourcePath(organization, project)
		if err != nil {
			return nil, err
		}
		if !authz.HasMethodAccess(ctx, objectMethodRead, []string{"/programs", "/data_file"}) && !authz.HasAnyMethodAccess(ctx, []string{resource}, objectMethodRead) {
			return nil, common.ErrUnauthorized
		}
	}
	return store.ListStorageCleanupRecords(ctx, organization, project, pathPrefix)
}
