package internaldrs

import (
	"context"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/authz"
	intcommon "github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/repair"
	"github.com/gofiber/fiber/v3"
)

func handleInternalStorageCleanupAuditFiber(om *core.ObjectManager) fiber.Handler {
	svc := repair.NewStorageCleanupService(om)
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req repair.StorageCleanupAuditRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		req.Organization = strings.TrimSpace(req.Organization)
		req.Project = strings.TrimSpace(req.Project)
		req.PathPrefix = strings.TrimSpace(req.PathPrefix)
		if req.Organization == "" || req.Project == "" {
			return apiutil.Reject(c, fiber.StatusBadRequest, "organization and project are required")
		}
		if err := authorizeStorageCleanupScope(c.Context(), req.Organization, req.Project, "read"); err != nil {
			return apiutil.HandleError(c, err)
		}
		report, err := svc.Audit(c.Context(), req)
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.JSON(report)
	}
}

func handleInternalStorageCleanupApplyFiber(om *core.ObjectManager) fiber.Handler {
	svc := repair.NewStorageCleanupService(om)
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		var req repair.StorageCleanupApplyRequest
		if err := decodeStrictJSON(c.Body(), &req); err != nil {
			return apiutil.Reject(c, fiber.StatusBadRequest, "Invalid request body: "+err.Error())
		}
		req.Organization = strings.TrimSpace(req.Organization)
		req.Project = strings.TrimSpace(req.Project)
		req.PathPrefix = strings.TrimSpace(req.PathPrefix)
		if req.Organization == "" || req.Project == "" {
			return apiutil.Reject(c, fiber.StatusBadRequest, "organization and project are required")
		}
		if err := authorizeStorageCleanupScope(c.Context(), req.Organization, req.Project, "read", "delete"); err != nil {
			return apiutil.HandleError(c, err)
		}
		result, err := svc.Apply(c.Context(), req)
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.JSON(result)
	}
}

func authorizeStorageCleanupScope(ctx context.Context, organization, project string, methods ...string) error {
	if !authz.IsAuthzEnforced(ctx) {
		return nil
	}
	resource, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return err
	}
	if authz.HasMethodAccess(ctx, methods[0], []string{"/programs", "/data_file"}) || authz.HasAnyMethodAccess(ctx, []string{resource}, methods...) {
		return nil
	}
	return intcommon.ErrUnauthorized
}
