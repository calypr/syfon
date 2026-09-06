package internaldrs

import (
	"strings"

	"github.com/gofiber/fiber/v3"

	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/faults"
	apimiddleware "github.com/calypr/syfon/internal/httpapi/middleware"
	"github.com/calypr/syfon/internal/maintenance/projectstorage"
)

type projectCleanupResponse struct {
	Organization        string `json:"organization"`
	ProjectID           string `json:"project_id"`
	DeletedObjects      int    `json:"deleted_objects"`
	DeletedBucketScopes int    `json:"deleted_bucket_scopes"`
}

func handleInternalDeleteProjectFiber(c fiber.Ctx, service *projectstorage.Service) error {
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

	result, err := service.DeleteProjectData(c.Context(), organization, projectID)
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	return c.JSON(projectCleanupResponse{
		Organization:        result.Organization,
		ProjectID:           result.ProjectID,
		DeletedObjects:      result.DeletedObjects,
		DeletedBucketScopes: result.DeletedBucketScopes,
	})
}
