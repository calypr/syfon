package internaldrs

import (
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

func RegisterInternalIndexRoutes(router fiber.Router, om *core.ObjectManager) {
	router.Get("/", handleInternalListFiber(om))
	router.Get(common.RouteInternalIndex, handleInternalListFiber(om))
	router.Get(fiberRoutePath(common.RouteInternalIndexDetail), handleInternalGetFiber(om))

	router.Post(common.RouteInternalIndex, handleInternalCreateFiber(om))
	router.Put(fiberRoutePath(common.RouteInternalIndexDetail), func(c fiber.Ctx) error { return handleInternalUpdateFiber(c, om) })
	router.Delete(fiberRoutePath(common.RouteInternalIndexDetail), handleInternalDeleteFiber(om))
	router.Delete("/", handleInternalDeleteByQueryFiber(om))
	router.Delete(common.RouteInternalIndex, handleInternalDeleteByQueryFiber(om))

	router.Post(common.RouteInternalBulkHashes, handleInternalBulkHashesFiber(om))
	router.Post(common.RouteInternalBulkSHA256, handleInternalBulkHashesFiber(om))
	router.Post(common.RouteInternalBulkCreate, handleInternalBulkCreateFiber(om))
	router.Post(common.RouteInternalBulkDocs, handleInternalBulkDocumentsFiber(om))
	router.Post(common.RouteInternalBulkDeleteHashes, handleInternalBulkDeleteFiber(om))
}

func handleInternalGetFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")
		obj, err := om.GetObject(c.Context(), id, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.JSON(obj)
	}
}

func handleInternalListFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		hash := c.Query("hash")
		hashType := c.Query("hash_type")

		if hash != "" {
			hashType, hash = common.ParseHashQuery(hash, hashType)
			objs, err := om.GetObjectsByChecksum(c.Context(), hash)
			if err != nil {
				return apiutil.HandleError(c, err)
			}

			filterOrg := strings.TrimSpace(c.Query("organization"))
			filterProject := strings.TrimSpace(c.Query("project"))

			records := make([]internalapi.InternalRecord, 0, len(objs))
			for _, o := range objs {
				if hashType != "" && !common.ObjectHasChecksumTypeAndValue(o, hashType, hash) {
					continue
				}
				if filterOrg != "" && !objectAuthzMatchesScope(o, filterOrg, filterProject) {
					continue
				}
				records = append(records, core.InternalObjectToInternalRecord(o))
			}
			return c.JSON(internalapi.ListRecordsResponse{Records: &records})
		}

		filterOrg, filterProject, hasScope, err := parseScopeQueryFiber(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		// List objects by scope (empty scope matches everything if no hash specified).
		if !hasScope {
			filterOrg, filterProject = "", ""
		}
		ids, err := om.ListObjectIDsByScope(c.Context(), filterOrg, filterProject)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		records := make([]internalapi.InternalRecord, 0, len(ids))
		for _, id := range ids {
			obj, err := om.GetObject(c.Context(), id, "read")
			if err != nil {
				continue // Skip if access denied or deleted
			}
			records = append(records, core.InternalObjectToInternalRecord(*obj))
		}

		return c.JSON(internalapi.ListRecordsResponse{Records: &records})
	}
}

func handleInternalDeleteFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")
		// Check for existence and permission
		if _, err := om.GetObject(c.Context(), id, "delete"); err != nil {
			return apiutil.HandleError(c, err)
		}
		if err := om.DeleteObject(c.Context(), id); err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleInternalCreateFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var bulkReq internalapi.BulkCreateRequest
		var candidates []models.InternalObject
		now := time.Now().UTC()

		if err := c.Bind().JSON(&bulkReq); err == nil && len(bulkReq.Records) > 0 {
			for i, r := range bulkReq.Records {
				obj, err := core.InternalRecordToInternalObject(r, now)
				if err != nil {
					return c.Status(fiber.StatusBadRequest).SendString(fmt.Sprintf("record[%d] invalid: %v", i, err))
				}
				candidates = append(candidates, obj)
			}
		} else {
			// Try single record fallback
			var singleReq internalapi.InternalRecord
			if err := c.Bind().JSON(&singleReq); err == nil && singleReq.Did != "" {
				obj, err := core.InternalRecordToInternalObject(singleReq, now)
				if err != nil {
					return c.Status(fiber.StatusBadRequest).SendString(fmt.Sprintf("record invalid: %v", err))
				}
				candidates = append(candidates, obj)
			}
		}

		if len(candidates) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: no records found")
		}

		if err := om.RegisterObjects(c.Context(), candidates); err != nil {
			return apiutil.HandleError(c, err)
		}

		// Return based on request type: single object for POST /index, bulk for /index/bulk
		if strings.HasSuffix(c.Path(), "/bulk") {
			records := make([]internalapi.InternalRecord, len(candidates))
			for i, cand := range candidates {
				records[i] = core.InternalObjectToInternalRecord(cand)
			}
			return c.Status(fiber.StatusCreated).JSON(internalapi.ListRecordsResponse{Records: &records})
		}

		// Single record response
		return c.Status(fiber.StatusCreated).JSON(core.InternalObjectToInternalRecordResponse(candidates[0]))
	}
}

func handleInternalDeleteByQueryFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := requireGen3AuthFiber(c); err != nil {
			return err
		}

		org, project, hasScope, err := parseScopeQueryFiber(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		if !hasScope {
			return c.Status(fiber.StatusBadRequest).SendString("No scope specified")
		}

		scope, err := scopeResource(org, project)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		if !resourceAllowed(c.Context(), scope, "delete") {
			return c.SendStatus(fiber.StatusForbidden)
		}

		count, err := om.DeleteBulkByScope(c.Context(), org, project)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.JSON(internalapi.DeleteByQueryResponse{Deleted: &count})
	}
}

func handleInternalBulkHashesFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkHashesRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		normalized := make([]string, 0, len(req.Hashes))
		for _, h := range req.Hashes {
			_, val := common.ParseHashQuery(h, "")
			normalized = append(normalized, val)
		}

		res, err := om.GetObjectsByChecksums(c.Context(), normalized)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		// Re-map back to original requested hash keys and filter by type if needed
		finalRes := make(map[string][]models.InternalObject, len(req.Hashes))
		for i, h := range req.Hashes {
			typ, val := common.ParseHashQuery(h, "")
			matches := res[normalized[i]]
			if typ != "" {
				filtered := make([]models.InternalObject, 0, len(matches))
				for _, m := range matches {
					if common.ObjectHasChecksumTypeAndValue(m, typ, val) {
						filtered = append(filtered, m)
					}
				}
				matches = filtered
			}
			finalRes[h] = matches
		}

		return c.JSON(struct {
			Results map[string][]models.InternalObject
		}{Results: finalRes})
	}
}

func handleInternalBulkCreateFiber(om *core.ObjectManager) fiber.Handler {
	return handleInternalCreateFiber(om)
}

func handleInternalBulkDocumentsFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkDocumentsRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		var ids []string
		if arr, err := req.AsBulkDocumentsRequest0(); err == nil {
			ids = append(ids, arr...)
		}
		if obj, err := req.AsBulkDocumentsRequest1(); err == nil {
			ids = append(ids, common.DerefStringSlice(obj.Ids)...)
			ids = append(ids, common.DerefStringSlice(obj.Dids)...)
		}
		if len(ids) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: ids are required")
		}

		records, err := om.GetBulkObjects(c.Context(), ids)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		out := make([]internalapi.InternalRecordResponse, 0, len(records))
		for _, obj := range records {
			out = append(out, core.InternalObjectToInternalRecordResponse(obj))
		}

		return c.JSON(out)
	}
}

func handleInternalBulkDeleteFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := requireGen3AuthFiber(c); err != nil {
			return err
		}

		var req internalapi.BulkHashesRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		if len(req.Hashes) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: hashes are required")
		}

		normalized := make([]string, 0, len(req.Hashes))
		for _, h := range req.Hashes {
			_, val := common.ParseHashQuery(h, "")
			if strings.TrimSpace(val) == "" {
				continue
			}
			normalized = append(normalized, val)
		}
		if len(normalized) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: hashes are required")
		}

		matches, err := om.GetObjectsByChecksums(c.Context(), normalized)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		toDelete := make([]string, 0)
		seen := make(map[string]struct{})
		for _, hash := range normalized {
			for _, obj := range matches[hash] {
				if !methodAllowedForAuthorizations(c.Context(), "delete", obj.Authorizations) {
					continue
				}
				if _, ok := seen[obj.Id]; ok {
					continue
				}
				seen[obj.Id] = struct{}{}
				toDelete = append(toDelete, obj.Id)
			}
		}

		if len(toDelete) == 0 {
			return c.JSON(internalapi.DeleteByQueryResponse{Deleted: common.Ptr(0)})
		}

		if err := om.BulkDeleteObjects(c.Context(), toDelete); err != nil {
			return apiutil.HandleError(c, err)
		}

		deleted := len(toDelete)
		return c.JSON(internalapi.DeleteByQueryResponse{Deleted: &deleted})
	}
}

func handleInternalUpdateFiber(c fiber.Ctx, om *core.ObjectManager) error {
	id := c.Params("id")
	var req models.InternalObject
	if err := c.Bind().JSON(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
	}

	existing, err := om.GetObject(c.Context(), id, "update")
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	merged, err := core.MergeInternalObjectUpdate(*existing, req, id, time.Now().UTC())
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	if err := om.RegisterObjects(c.Context(), []models.InternalObject{merged}); err != nil {
		return apiutil.HandleError(c, err)
	}

	return c.JSON(merged)
}
