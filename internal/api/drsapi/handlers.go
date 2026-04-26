package drsapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

// RegisterDRSRoutes binds standard GA4GH DRS handlers to the router.
func RegisterDRSRoutes(router fiber.Router, om *core.ObjectManager) {
	// Static routes first
	router.Post("/objects/register", handleRegisterObjectsFiber(om))
	router.Post("/objects/access", handleGetBulkAccessURLFiber(om))
	router.Post("/objects/delete", handleBulkDeleteObjectsFiber(om))
	router.Post("/objects/access-methods", handleUpdateAccessMethodsFiber(om))
	router.Get("/objects/checksum/:checksum", handleGetObjectsByChecksumFiber(om))
	router.Post("/objects", handleGetBulkObjectsFiber(om))
	router.Get("/service-info", handleGetServiceInfoFiber(om))
	router.Post("/upload-request", handleUploadRequestFiber(om))

	// Dynamic routes with parameters last
	router.Get("/objects/:object_id", handleGetObjectFiber(om))
	router.Post("/objects/:object_id", handleGetObjectFiber(om))
	router.Delete("/objects/:object_id", handleDeleteObjectFiber(om))
	router.Post("/objects/:object_id/delete", handleDeleteObjectFiber(om))
	router.Get("/objects/:object_id/access/:access_id", handleGetAccessURLFiber(om))
	router.Post("/objects/:object_id/access/:access_id", handleGetAccessURLFiber(om))
	router.Post("/objects/:object_id/access-methods", handleUpdateAccessMethodsFiber(om))

	// Options
	router.Options("/objects", handleOptionsBulkObjectFiber(om))
	router.Options("/objects/:object_id", handleOptionsBulkObjectFiber(om))
}

func handleGetObjectFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("object_id")
		obj, err := om.GetObject(c.Context(), id, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.JSON(obj.DrsObject)
	}
}

func handleGetAccessURLFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("object_id")
		accessID := c.Params("access_id")

		obj, err := om.GetObject(c.Context(), id, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		if obj.AccessMethods == nil {
			return c.Status(fiber.StatusNotFound).JSON(drs.Error{Msg: common.Ptr("No access methods found")})
		}

		var targetURL string
		for _, am := range *obj.AccessMethods {
			if strings.EqualFold(common.StringVal(am.AccessId), accessID) {
				if am.AccessUrl != nil {
					targetURL = am.AccessUrl.Url
				}
				break
			}
		}

		if targetURL == "" {
			return c.Status(fiber.StatusNotFound).JSON(drs.Error{Msg: common.Ptr("Access ID not found or has no URL")})
		}

		signed, err := om.SignURL(c.Context(), targetURL, urlmanager.SignOptions{Method: http.MethodGet})
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.JSON(drs.AccessURL{Url: signed})
	}
}

func handleRegisterObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body struct {
			Candidates []drs.DrsObjectCandidate `json:"candidates"`
		}
		if err := c.Bind().JSON(&body); err != nil || len(body.Candidates) == 0 {
			var single drs.DrsObjectCandidate
			if err2 := c.Bind().JSON(&single); err2 == nil && len(single.Checksums) > 0 {
				internalObj, err := core.CandidateToInternalObject(single, time.Now().UTC())
				if err != nil {
					return apiutil.HandleError(c, err)
				}
				if err := om.RegisterObjects(c.Context(), []models.InternalObject{internalObj}); err != nil {
					return apiutil.HandleError(c, err)
				}
				// Fetch back for full population (SelfUri, and access methods)
				finalObj, _ := om.GetObject(c.Context(), internalObj.Id, "read")
				if finalObj == nil {
					finalObj = &internalObj
				}
				return c.Status(fiber.StatusCreated).JSON(drs.N201ObjectsCreated{
					Objects: []drs.DrsObject{finalObj.DrsObject},
				})
			}
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		// List of internal objects to register
		toRegister := make([]models.InternalObject, 0, len(body.Candidates))
		for _, cand := range body.Candidates {
			internalObj, err := core.CandidateToInternalObject(cand, time.Now().UTC())
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			toRegister = append(toRegister, internalObj)
		}

		if err := om.RegisterObjects(c.Context(), toRegister); err != nil {
			return apiutil.HandleError(c, err)
		}

		// Reconstruct registered objects summary for response
		registered := make([]drs.DrsObject, len(toRegister))
		for i, internal := range toRegister {
			// Fetch back to ensure full population
			obj, err := om.GetObject(c.Context(), internal.Id, "read")
			if err == nil {
				registered[i] = obj.DrsObject
			} else {
				// Fallback to what we have if fetch fails
				registered[i] = internal.DrsObject
			}
		}

		return c.Status(fiber.StatusCreated).JSON(drs.N201ObjectsCreated{Objects: registered})
	}
}

func handleGetBulkObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body struct {
			BulkObjectIds []string `json:"bulk_object_ids"`
		}
		if err := c.Bind().JSON(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		resolved := make([]drs.DrsObject, 0)
		for _, id := range body.BulkObjectIds {
			obj, err := om.GetObject(c.Context(), id, "read")
			if err == nil {
				resolved = append(resolved, obj.DrsObject)
			}
		}

		return c.JSON(drs.N200OkDrsObjectsJSONResponse{
			ResolvedDrsObject: &resolved,
			Summary: &drs.Summary{
				Requested: common.Ptr(len(body.BulkObjectIds)),
				Resolved:  common.Ptr(len(resolved)),
			},
		})
	}
}

func handleGetBulkAccessURLFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.Status(fiber.StatusNotImplemented).JSON(drs.Error{Msg: common.Ptr("Bulk access lookup not implemented")})
	}
}

func handleGetObjectsByChecksumFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		checksum := c.Params("checksum")
		fetched, err := om.GetObjectsByChecksum(c.Context(), checksum)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		resolved := make([]drs.DrsObject, 0)
		for _, obj := range fetched {
			// Reuse GetObject to verify access consistently
			if _, err := om.GetObject(c.Context(), obj.Id, "read"); err == nil {
				resolved = append(resolved, obj.DrsObject)
			}
		}

		return c.JSON(drs.N200OkDrsObjectsJSONResponse{
			ResolvedDrsObject: &resolved,
			Summary: &drs.Summary{
				Requested: common.Ptr(1),
				Resolved:  common.Ptr(len(resolved)),
			},
		})
	}
}

func handleGetServiceInfoFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		info, err := om.GetServiceInfo(c.Context())
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.JSON(info)
	}
}

func handleUploadRequestFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if authz.IsGen3Mode(c.Context()) && !authz.HasAuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}

		var req drs.UploadRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}
		if len(req.Requests) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		bucket, err := om.ResolveBucket(c.Context(), "")
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		responses := make([]drs.UploadResponseObject, 0, len(req.Requests))
		for _, item := range req.Requests {
			key := strings.TrimSpace(item.Name)
			if oid, ok := common.CanonicalSHA256(item.Checksums); ok && oid != "" {
				key = oid
			}
			if key == "" {
				return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
			}

			signedURL, err := om.SignURL(c.Context(), common.BucketToURL(bucket, key), urlmanager.SignOptions{
				Method: http.MethodPut,
			})
			if err != nil {
				return apiutil.HandleError(c, err)
			}

			method := drs.UploadMethod{
				Type: drs.Https,
				AccessUrl: struct {
					Headers *[]string `json:"headers,omitempty"`
					Url     string    `json:"url"`
				}{Url: signedURL},
			}

			responses = append(responses, drs.UploadResponseObject{
				Name:          item.Name,
				Size:          item.Size,
				MimeType:      item.MimeType,
				Checksums:     item.Checksums,
				Description:   item.Description,
				Aliases:       item.Aliases,
				UploadMethods: &[]drs.UploadMethod{method},
			})
		}

		return c.Status(fiber.StatusOK).JSON(drs.N200UploadRequest{Responses: responses})
	}
}

func handleDeleteObjectFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("object_id")
		if err := om.DeleteObject(c.Context(), id); err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleUpdateAccessMethodsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.Status(fiber.StatusNotImplemented).JSON(drs.Error{Msg: common.Ptr("Access method update not implemented")})
	}
}

func handleBulkDeleteObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.Status(fiber.StatusNotImplemented).JSON(drs.Error{Msg: common.Ptr("Bulk delete not implemented")})
	}
}

func handleOptionsBulkObjectFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.SendStatus(fiber.StatusNoContent)
	}
}
