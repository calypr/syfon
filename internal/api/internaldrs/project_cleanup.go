package internaldrs

import (
	"strings"

	"github.com/gofiber/fiber/v3"

	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/faults"
	apimiddleware "github.com/calypr/syfon/internal/httpapi/middleware"
)

type projectCleanupResponse struct {
	Organization        string `json:"organization"`
	ProjectID           string `json:"project_id"`
	DeletedObjects      int    `json:"deleted_objects"`
	DeletedBucketScopes int    `json:"deleted_bucket_scopes"`
}

func handleInternalDeleteProjectFiber(c fiber.Ctx, om *core.ObjectManager) error {
	organization := strings.TrimSpace(c.Params("organization"))
	projectID := strings.TrimSpace(c.Params("project_id"))
	if organization == "" || projectID == "" {
		return apiutil.Reject(c, fiber.StatusBadRequest, "organization and project_id are required")
	}
	if apimiddleware.MissingGen3AuthHeader(c.Context()) {
		return apiutil.HandleError(c, faults.ErrUnauthorized)
	}
	if err := authorizeBucketScopeWrite(c.Context(), organization, projectID, "delete", "update"); err != nil {
		return apiutil.HandleError(c, err)
	}

	deletedObjects, err := om.DeleteBulkByScope(c.Context(), organization, projectID)
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	scopes, err := om.ListBucketScopes(c.Context())
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	deletedScopes := 0
	for _, scope := range scopes {
		if strings.TrimSpace(scope.Organization) != organization || strings.TrimSpace(scope.ProjectID) != projectID {
			continue
		}
		credentialID := strings.TrimSpace(scope.CredentialID)
		if credentialID == "" {
			credentialID = strings.TrimSpace(scope.Bucket)
		}
		if credentialID == "" {
			continue
		}
		if err := om.DeleteBucketScope(c.Context(), organization, projectID, credentialID, scope.PathPrefix); err != nil {
			return apiutil.HandleError(c, err)
		}
		deletedScopes++
	}

	return c.JSON(projectCleanupResponse{
		Organization:        organization,
		ProjectID:           projectID,
		DeletedObjects:      deletedObjects,
		DeletedBucketScopes: deletedScopes,
	})
}
