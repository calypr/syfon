package drsapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/api/attribution"
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
		return c.JSON(drsObjectPayload(*obj))
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

		targetURL := accessURLForID(obj, accessID)
		if targetURL == "" {
			return c.Status(fiber.StatusNotFound).JSON(drs.Error{Msg: common.Ptr("Access ID not found or has no URL")})
		}

		signed, err := om.SignURL(c.Context(), targetURL, urlmanager.SignOptions{Method: http.MethodGet})
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
			AccessID:   accessID,
			StorageURL: targetURL,
		})

		return c.JSON(drs.AccessURL{Url: signed})
	}
}

func accessURLForID(obj *models.InternalObject, accessID string) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	for _, am := range *obj.AccessMethods {
		if strings.EqualFold(common.StringVal(am.AccessId), accessID) && am.AccessUrl != nil {
			return am.AccessUrl.Url
		}
	}
	return ""
}

func handleRegisterObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body registerObjectsRequest
		if err := json.Unmarshal(c.Body(), &body); err != nil || len(body.Candidates) == 0 {
			var single registerObjectCandidate
			if err2 := json.Unmarshal(c.Body(), &single); err2 == nil && len(single.Checksums) > 0 {
				internalObj, err := registerCandidateToInternalObject(single, time.Now().UTC())
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
				return c.Status(fiber.StatusCreated).JSON(fiber.Map{
					"objects": []any{drsObjectPayload(*finalObj)},
				})
			}
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		// List of internal objects to register
		toRegister := make([]models.InternalObject, 0, len(body.Candidates))
		for _, cand := range body.Candidates {
			internalObj, err := registerCandidateToInternalObject(cand, time.Now().UTC())
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			toRegister = append(toRegister, internalObj)
		}

		if err := om.RegisterObjects(c.Context(), toRegister); err != nil {
			return apiutil.HandleError(c, err)
		}

		// Reconstruct registered objects summary for response
		registered := make([]any, len(toRegister))
		for i, internal := range toRegister {
			// Fetch back to ensure full population
			obj, err := om.GetObject(c.Context(), internal.Id, "read")
			if err == nil {
				registered[i] = drsObjectPayload(*obj)
			} else {
				// Fallback to what we have if fetch fails
				registered[i] = drsObjectPayload(internal)
			}
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{"objects": registered})
	}
}

type registerObjectsRequest struct {
	Candidates []registerObjectCandidate `json:"candidates"`
}

type registerObjectCandidate struct {
	drs.DrsObjectCandidate
	Auth models.AuthPathMap `json:"auth,omitempty"`
}

func registerCandidateToInternalObject(c registerObjectCandidate, now time.Time) (models.InternalObject, error) {
	obj, err := core.CandidateToInternalObject(c.DrsObjectCandidate, now)
	if err != nil {
		return models.InternalObject{}, err
	}
	if len(c.Auth) == 0 {
		return obj, nil
	}

	obj.Auth = c.Auth
	obj.Authorizations = models.AuthPathMapToAuthorizations(c.Auth)
	methods := core.AccessMethodsFromAuthPathMap(c.Auth)
	if len(methods) > 0 {
		obj.AccessMethods = &methods
	}
	return obj, nil
}

func handleGetBulkObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body struct {
			BulkObjectIds []string `json:"bulk_object_ids"`
		}
		if err := c.Bind().JSON(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		resolved := make([]any, 0)
		for _, id := range body.BulkObjectIds {
			obj, err := om.GetObject(c.Context(), id, "read")
			if err == nil {
				resolved = append(resolved, drsObjectPayload(*obj))
			}
		}

		return c.JSON(fiber.Map{
			"resolved_drs_object": resolved,
			"summary": drs.Summary{
				Requested: common.Ptr(len(body.BulkObjectIds)),
				Resolved:  common.Ptr(len(resolved)),
			},
		})
	}
}

func handleGetBulkAccessURLFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body drs.BulkObjectAccessId
		if err := c.Bind().JSON(&body); err != nil || body.BulkObjectAccessIds == nil {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		requested := 0
		resolved := make([]drs.BulkAccessURL, 0)
		unresolvedIDs := make([]string, 0)

		for _, item := range *body.BulkObjectAccessIds {
			objectID := strings.TrimSpace(common.StringVal(item.BulkObjectId))
			if objectID == "" || item.BulkAccessIds == nil || len(*item.BulkAccessIds) == 0 {
				requested++
				if objectID != "" {
					unresolvedIDs = append(unresolvedIDs, objectID)
				}
				continue
			}

			obj, err := om.GetObject(c.Context(), objectID, "read")
			if err != nil {
				requested += len(*item.BulkAccessIds)
				unresolvedIDs = append(unresolvedIDs, objectID)
				continue
			}

			for _, rawAccessID := range *item.BulkAccessIds {
				requested++
				accessID := strings.TrimSpace(rawAccessID)
				targetURL := accessURLForID(obj, accessID)
				if accessID == "" || targetURL == "" {
					unresolvedIDs = append(unresolvedIDs, objectID)
					continue
				}

				signed, err := om.SignURL(c.Context(), targetURL, urlmanager.SignOptions{Method: http.MethodGet})
				if err != nil {
					unresolvedIDs = append(unresolvedIDs, objectID)
					continue
				}
				attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
					AccessID:   accessID,
					StorageURL: targetURL,
				})
				resolved = append(resolved, drs.BulkAccessURL{
					DrsObjectId: common.Ptr(objectID),
					DrsAccessId: common.Ptr(accessID),
					Url:         signed,
				})
			}
		}

		resp := fiber.Map{
			"resolved_drs_object_access_urls": resolved,
			"summary": drs.Summary{
				Requested:  common.Ptr(requested),
				Resolved:   common.Ptr(len(resolved)),
				Unresolved: common.Ptr(requested - len(resolved)),
			},
		}
		if len(unresolvedIDs) > 0 {
			resp["unresolved_drs_objects"] = []fiber.Map{{
				"error_code": fiber.StatusNotFound,
				"object_ids": unresolvedIDs,
			}}
		}
		return c.JSON(resp)
	}
}

func handleGetObjectsByChecksumFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		checksum := c.Params("checksum")
		fetched, err := om.GetObjectsByChecksum(c.Context(), checksum)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		resolved := make([]any, 0)
		for _, obj := range fetched {
			// Reuse GetObject to verify access consistently
			if _, err := om.GetObject(c.Context(), obj.Id, "read"); err == nil {
				resolved = append(resolved, drsObjectPayload(obj))
			}
		}

		return c.JSON(fiber.Map{
			"resolved_drs_object": resolved,
			"summary": drs.Summary{
				Requested: common.Ptr(1),
				Resolved:  common.Ptr(len(resolved)),
			},
		})
	}
}

func drsObjectPayload(obj models.InternalObject) map[string]any {
	var payload map[string]any
	data, err := json.Marshal(obj.DrsObject)
	if err == nil {
		_ = json.Unmarshal(data, &payload)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	if len(obj.Auth) > 0 {
		payload["auth"] = obj.Auth
	}
	return payload
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
		objectID := strings.TrimSpace(c.Params("object_id"))
		if objectID != "" {
			var body drs.AccessMethodUpdateRequest
			if err := c.Bind().JSON(&body); err != nil || len(body.AccessMethods) == 0 {
				return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
			}
			if _, err := om.GetObject(c.Context(), objectID, "write"); err != nil {
				return apiutil.HandleError(c, err)
			}
			if err := om.UpdateObjectAccessMethods(c.Context(), objectID, body.AccessMethods); err != nil {
				return apiutil.HandleError(c, err)
			}
			obj, err := om.GetObject(c.Context(), objectID, "read")
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			return c.JSON(drsObjectPayload(*obj))
		}

		var body drs.BulkAccessMethodUpdateRequest
		if err := c.Bind().JSON(&body); err != nil || len(body.Updates) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}

		updates := make(map[string][]drs.AccessMethod, len(body.Updates))
		orderedIDs := make([]string, 0, len(body.Updates))
		for _, update := range body.Updates {
			id := strings.TrimSpace(update.ObjectId)
			if id == "" || len(update.AccessMethods) == 0 {
				return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
			}
			if _, err := om.GetObject(c.Context(), id, "write"); err != nil {
				return apiutil.HandleError(c, err)
			}
			if _, exists := updates[id]; !exists {
				orderedIDs = append(orderedIDs, id)
			}
			updates[id] = update.AccessMethods
		}

		if err := om.BulkUpdateAccessMethods(c.Context(), updates); err != nil {
			return apiutil.HandleError(c, err)
		}

		objects := make([]any, 0, len(orderedIDs))
		for _, id := range orderedIDs {
			obj, err := om.GetObject(c.Context(), id, "read")
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			objects = append(objects, drsObjectPayload(*obj))
		}
		return c.JSON(fiber.Map{"objects": objects})
	}
}

func handleBulkDeleteObjectsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body drs.BulkDeleteRequest
		if err := c.Bind().JSON(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("Invalid request body")})
		}
		if len(body.BulkObjectIds) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("bulk_object_ids cannot be empty")})
		}

		ids := make([]string, 0, len(body.BulkObjectIds))
		seen := make(map[string]struct{}, len(body.BulkObjectIds))
		for _, rawID := range body.BulkObjectIds {
			id := strings.TrimSpace(rawID)
			if id == "" {
				return c.Status(fiber.StatusBadRequest).JSON(drs.Error{Msg: common.Ptr("bulk_object_ids cannot contain empty values")})
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if _, err := om.GetObject(c.Context(), id, "delete"); err != nil {
				return apiutil.HandleError(c, err)
			}
			ids = append(ids, id)
		}

		if err := om.BulkDeleteObjects(c.Context(), ids); err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleOptionsBulkObjectFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		return c.SendStatus(fiber.StatusNoContent)
	}
}
