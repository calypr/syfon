package drs

import (
	"net/http"
	"strings"

	generated "github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/httpapi/response"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

func handleGetAccessURLFiber(objectService *objects.Service, transferService *transfers.Service) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("object_id")
		accessID := c.Params("access_id")

		obj, err := objectService.GetObject(c.Context(), id, "read")
		if err != nil {
			return response.HandleError(c, err)
		}

		targetURL := accessURLForID(obj, accessID)
		if targetURL == "" {
			return c.Status(fiber.StatusNotFound).JSON(generated.Error{Msg: drsPtr("Access ID not found or has no URL")})
		}

		opts := storage.AccessOptions{Method: http.MethodGet}
		if obj.Name != nil {
			opts.DownloadFilename = storage.DownloadFilename(*obj.Name)
		}
		signed, err := transferService.SignObjectURL(c.Context(), obj, targetURL, opts)
		if err != nil {
			return response.HandleError(c, err)
		}
		if err := transferService.RecordAccessIssued(c.Context(), transfers.AccessRequest{
			Object:     obj,
			Direction:  usage.ProviderTransferDirectionDownload,
			AccessID:   accessID,
			StorageURL: targetURL,
		}); err != nil {
			return response.HandleError(c, err)
		}

		return c.JSON(generated.AccessURL{Url: signed})
	}
}

func accessURLForID(obj *objects.Record, accessID string) string {
	if obj == nil || obj.AccessMethods == nil {
		return ""
	}
	accessID = strings.TrimSpace(accessID)
	if accessID == "" {
		return ""
	}
	legacyMatches := make([]string, 0, 1)
	for _, am := range *obj.AccessMethods {
		if am.AccessUrl == nil || strings.TrimSpace(am.AccessUrl.Url) == "" {
			continue
		}
		if strings.EqualFold(drsStringValue(am.AccessId), accessID) {
			return am.AccessUrl.Url
		}
		if strings.EqualFold(strings.TrimSpace(string(am.Type)), accessID) {
			legacyMatches = append(legacyMatches, am.AccessUrl.Url)
		}
	}
	if len(legacyMatches) == 1 {
		return legacyMatches[0]
	}
	return ""
}

func handleGetBulkAccessURLFiber(objectService *objects.Service, transferService *transfers.Service) fiber.Handler {
	return func(c fiber.Ctx) error {
		var body generated.BulkObjectAccessId
		if err := c.Bind().JSON(&body); err != nil || body.BulkObjectAccessIds == nil {
			return c.Status(fiber.StatusBadRequest).JSON(generated.Error{Msg: drsPtr("Invalid request body")})
		}

		requested := 0
		resolved := make([]generated.BulkAccessURL, 0)
		unresolvedIDs := make([]string, 0)

		for _, item := range *body.BulkObjectAccessIds {
			objectID := strings.TrimSpace(drsStringValue(item.BulkObjectId))
			if objectID == "" || item.BulkAccessIds == nil || len(*item.BulkAccessIds) == 0 {
				requested++
				if objectID != "" {
					unresolvedIDs = append(unresolvedIDs, objectID)
				}
				continue
			}

			obj, err := objectService.GetObject(c.Context(), objectID, "read")
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

				opts := storage.AccessOptions{Method: http.MethodGet}
				if obj.Name != nil {
					opts.DownloadFilename = storage.DownloadFilename(*obj.Name)
				}
				signed, err := transferService.SignObjectURL(c.Context(), obj, targetURL, opts)
				if err != nil {
					unresolvedIDs = append(unresolvedIDs, objectID)
					continue
				}
				if err := transferService.RecordAccessIssued(c.Context(), transfers.AccessRequest{
					Object:     obj,
					Direction:  usage.ProviderTransferDirectionDownload,
					AccessID:   accessID,
					StorageURL: targetURL,
				}); err != nil {
					unresolvedIDs = append(unresolvedIDs, objectID)
					continue
				}
				resolved = append(resolved, generated.BulkAccessURL{
					DrsObjectId: drsPtr(objectID),
					DrsAccessId: drsPtr(accessID),
					Url:         signed,
				})
			}
		}

		resp := fiber.Map{
			"resolved_drs_object_access_urls": resolved,
			"summary": generated.Summary{
				Requested:  drsPtr(requested),
				Resolved:   drsPtr(len(resolved)),
				Unresolved: drsPtr(requested - len(resolved)),
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
