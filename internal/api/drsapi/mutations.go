package drsapi

import (
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

func handleUploadRequestFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
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
			ids = append(ids, id)
		}

		if err := om.BulkDeleteObjects(c.Context(), ids); err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}
