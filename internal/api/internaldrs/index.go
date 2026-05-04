package internaldrs

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/internalapi"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/api/apiutil"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

const (
	defaultInternalListLimit = 1000
	maxInternalListLimit     = 1024
)

func RegisterInternalRoutes(router fiber.Router, om *core.ObjectManager) {
	router.Get("/", handleInternalListFiber(om))
	router.Get(common.RouteInternalIndex, handleInternalListFiber(om))
	router.Get(routeutil.FiberPath(common.RouteInternalIndexDetail), handleInternalGetFiber(om))

	router.Post(common.RouteInternalIndex, handleInternalCreateFiber(om))
	router.Put(routeutil.FiberPath(common.RouteInternalIndexDetail), func(c fiber.Ctx) error { return handleInternalUpdateFiber(c, om) })
	router.Delete(routeutil.FiberPath(common.RouteInternalIndexDetail), handleInternalDeleteFiber(om))
	router.Delete("/", handleInternalDeleteByQueryFiber(om))
	router.Delete(common.RouteInternalIndex, handleInternalDeleteByQueryFiber(om))

	router.Post(common.RouteInternalBulkHashes, handleInternalBulkHashesFiber(om))
	router.Post(common.RouteInternalBulkSHA256, handleInternalBulkSHA256ValidityFiber(om))
	router.Post(common.RouteInternalBulkCreate, handleInternalBulkCreateFiber(om))
	router.Post(common.RouteInternalBulkDocs, handleInternalBulkDocumentsFiber(om))
	router.Post(common.RouteInternalBulkDeleteHashes, handleInternalBulkDeleteFiber(om))

	registerInternalTransferRoutes(router, om)
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
		objectURL := strings.TrimSpace(c.Query("url"))

		if hash != "" {
			hashType, hash = common.ParseHashQuery(hash, hashType)
			filterOrg := strings.TrimSpace(c.Query("organization"))
			filterProject := strings.TrimSpace(c.Query("project"))
			limit, start, offset, err := parseInternalListPaginationFiber(c)
			if err != nil {
				return c.Status(fiber.StatusBadRequest).SendString(err.Error())
			}
			ids, err := om.ListObjectIDsPageByChecksum(c.Context(), hash, hashType, filterOrg, filterProject, "read", start, limit, offset)
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			objs, err := om.GetBulkObjects(c.Context(), ids, "read")
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			records := make([]internalapi.InternalRecord, 0, len(objs))
			for _, o := range objs {
				records = append(records, core.InternalObjectToInternalRecord(o))
			}
			return c.JSON(internalapi.ListRecordsResponse{Records: &records})
		}

		filterOrg, filterProject, hasScope, err := parseScopeQueryParts(c.Query("organization"), c.Query("program"), c.Query("project"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		if !hasScope {
			filterOrg, filterProject = "", ""
		}

		limit, start, offset, err := parseInternalListPaginationFiber(c)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}

		var ids []string
		if objectURL != "" {
			ids, err = om.ListObjectIDsPageByURL(c.Context(), objectURL, filterOrg, filterProject, "read", start, limit, offset)
		} else {
			ids, err = om.ListObjectIDsPageByScope(c.Context(), filterOrg, filterProject, "read", start, limit, offset)
		}
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		objs, err := om.GetBulkObjects(c.Context(), ids, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		records := make([]internalapi.InternalRecord, 0, len(objs))
		for _, obj := range objs {
			records = append(records, core.InternalObjectToInternalRecord(obj))
		}
		return c.JSON(internalapi.ListRecordsResponse{Records: &records})
	}
}

func handleInternalDeleteFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")
		if err := om.DeleteObject(c.Context(), id); err != nil {
			return apiutil.HandleError(c, err)
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}

func handleInternalCreateFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		candidates, err := decodeInternalCreateCandidates(c, time.Now().UTC())
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: " + err.Error())
		}
		if err := om.RegisterObjects(c.Context(), candidates); err != nil {
			return apiutil.HandleError(c, err)
		}

		if strings.HasSuffix(c.Path(), "/bulk") {
			records := make([]internalapi.InternalRecord, len(candidates))
			for i, cand := range candidates {
				records[i] = core.InternalObjectToInternalRecord(cand)
			}
			return c.Status(fiber.StatusCreated).JSON(internalapi.ListRecordsResponse{Records: &records})
		}
		return c.Status(fiber.StatusCreated).JSON(core.InternalObjectToInternalRecordResponse(candidates[0]))
	}
}

func handleInternalDeleteByQueryFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}
		org, project, hasScope, err := parseScopeQueryParts(c.Query("organization"), c.Query("program"), c.Query("project"))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).SendString(err.Error())
		}
		if !hasScope {
			return c.Status(fiber.StatusBadRequest).SendString("No scope specified")
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

		normalized := normalizeBulkHashes(req.Hashes)
		res, err := om.GetObjectsByChecksums(c.Context(), normalized, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		finalRes := make(map[string][]models.InternalObject, len(req.Hashes))
		for i, h := range req.Hashes {
			typ, val := common.ParseHashQuery(h, "")
			matches := []models.InternalObject{}
			if i < len(normalized) {
				matches = res[normalized[i]]
			}
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

func handleInternalBulkSHA256ValidityFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkSHA256ValidityRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		if req.Sha256 == nil || len(*req.Sha256) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: sha256 values are required")
		}

		hashes := make([]string, 0, len(*req.Sha256))
		out := make(map[string]bool, len(*req.Sha256))
		for _, raw := range *req.Sha256 {
			hash := strings.TrimSpace(raw)
			if hash == "" {
				continue
			}
			hashes = append(hashes, hash)
			out[hash] = false
		}
		if len(hashes) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: sha256 values are required")
		}

		records, err := om.GetObjectsByChecksums(c.Context(), hashes, "read")
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		for _, hash := range hashes {
			for _, obj := range records[hash] {
				if common.ObjectHasChecksumTypeAndValue(obj, "sha256", hash) {
					out[hash] = true
					break
				}
			}
		}
		return c.JSON(out)
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

		records, err := om.GetBulkObjects(c.Context(), ids, "read")
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
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}

		var req internalapi.BulkHashesRequest
		if err := c.Bind().JSON(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		if len(req.Hashes) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: hashes are required")
		}

		normalized := normalizeNonEmptyBulkHashes(req.Hashes)
		if len(normalized) == 0 {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body: hashes are required")
		}

		deleted, err := om.DeleteObjectsByChecksums(c.Context(), normalized)
		if err != nil {
			return apiutil.HandleError(c, err)
		}
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
	if err := om.ReplaceObjects(c.Context(), []models.InternalObject{merged}); err != nil {
		return apiutil.HandleError(c, err)
	}
	return c.JSON(merged)
}

func parseScopeQueryParts(organization, program, project string) (string, string, bool, error) {
	org := strings.TrimSpace(organization)
	if org == "" {
		org = strings.TrimSpace(program)
	}
	project = strings.TrimSpace(project)
	if project != "" && org == "" {
		return "", "", false, fmt.Errorf("organization is required when project is set")
	}
	if org != "" {
		return org, project, true, nil
	}
	return "", "", false, nil
}

func parseScopeQuery(r *http.Request) (string, string, bool, error) {
	return parseScopeQueryParts(
		r.URL.Query().Get("organization"),
		r.URL.Query().Get("program"),
		r.URL.Query().Get("project"),
	)
}

func paginateInternalListIDsFiber(c fiber.Ctx, ids []string) ([]string, error) {
	limit, start, offset, err := parseInternalListPaginationFiber(c)
	if err != nil {
		return nil, err
	}
	if limit == 0 || len(ids) == 0 {
		return []string{}, nil
	}
	if start != "" {
		offset = sort.SearchStrings(ids, start)
		for offset < len(ids) && ids[offset] <= start {
			offset++
		}
	}
	if offset >= len(ids) {
		return []string{}, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	return ids[offset:end], nil
}

func parseInternalListPaginationFiber(c fiber.Ctx) (int, string, int, error) {
	limit := defaultInternalListLimit
	rawLimit := strings.TrimSpace(c.Query("limit"))
	if rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil {
			return 0, "", 0, fmt.Errorf("limit must be an integer")
		}
		if parsed < 0 {
			return 0, "", 0, fmt.Errorf("limit must be >= 0")
		}
		limit = parsed
	}
	if limit > maxInternalListLimit {
		limit = maxInternalListLimit
	}

	start := strings.TrimSpace(c.Query("start"))
	offset := 0
	if start == "" {
		rawPage := strings.TrimSpace(c.Query("page"))
		if rawPage != "" {
			page, err := strconv.Atoi(rawPage)
			if err != nil {
				return 0, "", 0, fmt.Errorf("page must be an integer")
			}
			if page < 0 {
				return 0, "", 0, fmt.Errorf("page must be >= 0")
			}
			offset = page * limit
		}
	}
	return limit, start, offset, nil
}

func decodeInternalCreateCandidates(c fiber.Ctx, now time.Time) ([]models.InternalObject, error) {
	var bulkReq internalapi.BulkCreateRequest
	candidates := make([]models.InternalObject, 0)
	if err := c.Bind().JSON(&bulkReq); err == nil && len(bulkReq.Records) > 0 {
		for i, r := range bulkReq.Records {
			obj, err := core.InternalRecordToInternalObject(r, now)
			if err != nil {
				return nil, fmt.Errorf("record[%d] invalid: %w", i, err)
			}
			candidates = append(candidates, obj)
		}
		return candidates, nil
	}

	var singleReq internalapi.InternalRecord
	if err := c.Bind().JSON(&singleReq); err == nil && singleReq.Did != "" {
		obj, err := core.InternalRecordToInternalObject(singleReq, now)
		if err != nil {
			return nil, fmt.Errorf("record invalid: %w", err)
		}
		candidates = append(candidates, obj)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no records found")
	}
	return candidates, nil
}

func normalizeBulkHashes(hashes []string) []string {
	normalized := make([]string, 0, len(hashes))
	for _, h := range hashes {
		_, val := common.ParseHashQuery(h, "")
		normalized = append(normalized, val)
	}
	return normalized
}

func normalizeNonEmptyBulkHashes(hashes []string) []string {
	normalized := make([]string, 0, len(hashes))
	for _, h := range hashes {
		_, val := common.ParseHashQuery(h, "")
		if strings.TrimSpace(val) == "" {
			continue
		}
		normalized = append(normalized, val)
	}
	return normalized
}

func objectAuthzMatchesScope(obj models.InternalObject, org, project string) bool {
	authzMap := sycommon.ControlledAccessToAuthzMap(core.ObjectAccessResources(&obj))
	return sycommon.AuthzMapMatchesScope(authzMap, org, project)
}
