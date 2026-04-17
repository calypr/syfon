package internaldrs

import (
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"

	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

// RegisterInternalIndexRoutes registers the Internal-compatible routes on the router.
func RegisterInternalIndexRoutes(router fiber.Router, database db.DatabaseInterface, uM ...urlmanager.UrlManager) {
	// Index Routes (Legacy/Internal)
	router.Get(fiberRoutePath("/index"), handleInternalListFiber(database))
	router.Post(fiberRoutePath("/index"), handleInternalCreateFiber(database))
	router.Delete(fiberRoutePath("/index"), handleInternalDeleteByQueryFiber(database))

	router.Get(fiberRoutePath("/index/{id}"), handleInternalGet(database))
	router.Put(fiberRoutePath("/index/{id}"), handleInternalUpdate(database))
	router.Delete(fiberRoutePath("/index/{id}"), handleInternalDelete(database))

	// Bulk Routes
	router.Post(fiberRoutePath("/index/bulk"), handleInternalBulkCreateFiber(database))
	router.Post(fiberRoutePath("/index/bulk/hashes"), handleInternalBulkHashesFiber(database))
	router.Post(fiberRoutePath("/index/bulk/documents"), handleInternalBulkDocumentsFiber(database))
	router.Post(fiberRoutePath("/index/bulk/delete"), handleInternalBulkDeleteHashesFiber(database))
	router.Post(fiberRoutePath("/index/bulk/sha256/validity"), handleInternalBulkSHA256ValidityFiber(database))
}

// handleInternalGet retrieves a record by DID.
func handleInternalGet(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")
		obj, err := database.GetObject(c.Context(), id)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}
		return c.JSON(obj)
	}
}

// handleInternalCreate creates a new record.
func handleInternalCreateFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var obj models.InternalObject
		if err := c.Bind().JSON(&obj); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", err)
		}

		if obj.Id == "" {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", nil)
		}
		if len(obj.Authorizations) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", nil)
		}

		aliased, canonicalObj, aliasErr := maybeAliasBySHA256(c.Context(), database, &obj)
		if aliasErr != nil {
			return writeDBErrorFiber(c, aliasErr)
		}
		if aliased {
			canonicalObj.Id = obj.Id
			return c.Status(fiber.StatusCreated).JSON(canonicalObj)
		}

		if err := database.CreateObject(c.Context(), &obj); err != nil {
			return writeDBErrorFiber(c, err)
		}

		return c.Status(fiber.StatusCreated).JSON(obj)
	}
}

func handleInternalBulkHashesFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkHashesRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", err)
		}

		if len(req.Hashes) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "hashes are required", nil)
		}

		records := make([]internalapi.InternalRecord, 0, len(req.Hashes))
		for _, raw := range req.Hashes {
			hashType, hashValue := common.ParseHashQuery(raw, "")
			if hashValue == "" {
				continue
			}

			objs, err := database.GetObjectsByChecksum(c.Context(), hashValue)
			if err != nil {
				continue
			}

			for _, o := range objs {
				if !common.ObjectHasChecksumTypeAndValue(o, hashType, hashValue) {
					continue
				}
				// InternalObject can be converted to InternalRecord via JSON
				data, _ := json.Marshal(o)
				var rec internalapi.InternalRecord
				json.Unmarshal(data, &rec)
				records = append(records, rec)
			}
		}

		return c.JSON(internalapi.ListRecordsResponse{Records: &records})
	}
}

func handleInternalBulkCreateFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkCreateRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", err)
		}

		if len(req.Records) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "records are required", nil)
		}

		created := make([]internalapi.InternalRecord, 0, len(req.Records))
		for _, item := range req.Records {
			obj := models.InternalObject{}
			data, err := json.Marshal(item)
			if err != nil {
				continue
			}
			if err := json.Unmarshal(data, &obj); err != nil {
				continue
			}

			if obj.Id == "" {
				continue
			}

			// Check Authz
			if !authz.HasAnyMethodAccess(c.Context(), obj.Authorizations, "create", "file_upload") {
				continue
			}

			if err := database.CreateObject(c.Context(), &obj); err != nil {
				continue
			}

			var rec internalapi.InternalRecord
			json.Unmarshal(data, &rec)
			created = append(created, rec)
		}

		return c.Status(fiber.StatusCreated).JSON(internalapi.ListRecordsResponse{Records: &created})
	}
}

func maybeAliasBySHA256(ctx context.Context, database db.DatabaseInterface, obj *models.InternalObject) (bool, *models.InternalObject, error) {
	if obj == nil {
		return false, nil, nil
	}
	sha := ""
	for _, c := range obj.Checksums {
		if strings.EqualFold(c.Type, "sha256") || strings.EqualFold(c.Type, "sha-256") {
			sha = c.Checksum
			break
		}
	}

	if sha == "" {
		return false, nil, nil
	}

	existing, err := database.GetObjectsByChecksum(ctx, sha)
	if err != nil {
		return false, nil, err
	}
	if len(existing) == 0 {
		return false, nil, nil
	}
	sort.Slice(existing, func(i, j int) bool { return existing[i].Id < existing[j].Id })
	canonical := existing[0]
	if strings.TrimSpace(canonical.Id) == "" || canonical.Id == obj.Id {
		return false, nil, nil
	}
	if err := database.CreateObjectAlias(ctx, obj.Id, canonical.Id); err != nil {
		return false, nil, err
	}
	canonicalCopy := canonical
	return true, &canonicalCopy, nil
}

// handleInternalUpdate updates an existing record.
func handleInternalUpdate(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")

		var update models.InternalObject
		if err := c.Bind().JSON(&update); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", err)
		}

		existing, err := database.GetObject(c.Context(), id)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}

		now := time.Now().UTC()
		merged, err := mergeInternalObjectUpdate(*existing, update, id, now)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request body", err)
		}

		if err := database.RegisterObjects(c.Context(), []models.InternalObject{merged}); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "Failed to update object", err)
		}

		return c.Status(fiber.StatusOK).JSON(merged)
	}
}

func mergeInternalObjectUpdate(existing, update models.InternalObject, objectID string, updatedAt time.Time) (models.InternalObject, error) {
	merged := make(map[string]interface{})
	for k, v := range existing.Properties {
		merged[k] = v
	}
	for k, v := range update.Properties {
		merged[k] = v
	}
	merged["id"] = objectID
	merged["did"] = objectID
	merged["created_time"] = existing.CreatedTime.UTC().Format(time.RFC3339)
	merged["updated_time"] = updatedAt.UTC().Format(time.RFC3339)

	if existing.Name != nil {
		merged["name"] = *existing.Name
		merged["file_name"] = *existing.Name
	}
	if update.Name != nil {
		merged["name"] = *update.Name
		merged["file_name"] = *update.Name
	}
	if existing.Description != nil {
		merged["description"] = *existing.Description
	}
	if update.Description != nil {
		merged["description"] = *update.Description
	}
	if existing.MimeType != nil {
		merged["mime_type"] = *existing.MimeType
	}
	if update.MimeType != nil {
		merged["mime_type"] = *update.MimeType
	}
	if existing.Version != nil {
		merged["version"] = *existing.Version
	}
	if update.Version != nil {
		merged["version"] = *update.Version
	}
	if len(update.Authorizations) > 0 {
		merged["authorizations"] = append([]string(nil), update.Authorizations...)
		merged["authz"] = append([]string(nil), update.Authorizations...)
	} else if len(existing.Authorizations) > 0 {
		merged["authorizations"] = append([]string(nil), existing.Authorizations...)
		merged["authz"] = append([]string(nil), existing.Authorizations...)
	}

	if len(update.Checksums) > 0 {
		merged["checksums"] = append([]drs.Checksum(nil), update.Checksums...)
		hashes := make(map[string]string, len(update.Checksums))
		for _, checksum := range update.Checksums {
			if checksum.Type == "" || checksum.Checksum == "" {
				continue
			}
			hashes[checksum.Type] = checksum.Checksum
		}
		if len(hashes) > 0 {
			merged["hashes"] = hashes
		}
	} else if len(existing.Checksums) > 0 {
		merged["checksums"] = append([]drs.Checksum(nil), existing.Checksums...)
		hashes := make(map[string]string, len(existing.Checksums))
		for _, checksum := range existing.Checksums {
			if checksum.Type == "" || checksum.Checksum == "" {
				continue
			}
			hashes[checksum.Type] = checksum.Checksum
		}
		if len(hashes) > 0 {
			merged["hashes"] = hashes
		}
	}

	if update.AccessMethods != nil {
		merged["access_methods"] = append([]drs.AccessMethod(nil), (*update.AccessMethods)...)
	} else if existing.AccessMethods != nil {
		merged["access_methods"] = append([]drs.AccessMethod(nil), (*existing.AccessMethods)...)
	}
	if update.Size > 0 {
		merged["size"] = update.Size
	} else if existing.Size > 0 {
		merged["size"] = existing.Size
	}

	data, err := json.Marshal(merged)
	if err != nil {
		return models.InternalObject{}, err
	}

	var out models.InternalObject
	if err := json.Unmarshal(data, &out); err != nil {
		return models.InternalObject{}, err
	}
	out.Id = objectID
	out.SelfUri = "drs://" + objectID
	out.UpdatedTime = &updatedAt
	return out, nil
}

// handleInternalDelete deletes a record.
func handleInternalDelete(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		id := c.Params("id")
		targetID := id
		if canonicalID, aliasErr := database.ResolveObjectAlias(c.Context(), id); aliasErr == nil && strings.TrimSpace(canonicalID) != "" {
			targetID = strings.TrimSpace(canonicalID)
		} else if aliasErr != nil && !errors.Is(aliasErr, common.ErrNotFound) {
			return writeDBErrorFiber(c, aliasErr)
		}

		if err := database.DeleteObject(c.Context(), targetID); err != nil {
			return writeDBErrorFiber(c, err)
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

func parseScopeQueryFiber(c fiber.Ctx) (string, bool, error) {
	authz := strings.TrimSpace(c.Query("authz"))
	if authz != "" {
		return authz, true, nil
	}
	org := strings.TrimSpace(c.Query("organization"))
	if org == "" {
		org = strings.TrimSpace(c.Query("program"))
	}
	project := strings.TrimSpace(c.Query("project"))
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := common.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func handleInternalDeleteByQueryFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		scopePrefix, hasScope, err := parseScopeQueryFiber(c)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, err.Error(), err)
		}

		hash := c.Query("hash")
		hashType := c.Query("hash_type")

		if !hasScope && hash == "" {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "organization/project, authz, or hash query is required", nil)
		}
		if authz.IsGen3Mode(c.Context()) && !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}

		var ids []string
		if hasScope {
			scopeIDs, err := database.ListObjectIDsByResourcePrefix(c.Context(), scopePrefix)
			if err != nil {
				return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "failed to list records by scope", err)
			}
			ids = append(ids, scopeIDs...)
		}

		if hash != "" {
			hashType, hash = common.ParseHashQuery(hash, hashType)
			objs, err := database.GetObjectsByChecksum(c.Context(), hash)
			if err != nil {
				return writeDBErrorFiber(c, err)
			}
			for _, o := range objs {
				if hashType != "" && !common.ObjectHasChecksumTypeAndValue(o, hashType, hash) {
					continue
				}
				ids = append(ids, o.Id)
			}
		}

		toDelete := make([]string, 0, len(ids))
		for _, id := range ids {
			obj, err := database.GetObject(c.Context(), id)
			if err != nil {
				if errors.Is(err, common.ErrNotFound) {
					continue
				}
				return writeDBErrorFiber(c, err)
			}
			if !authz.HasMethodAccess(c.Context(), "delete", obj.Authorizations) {
				return writeAuthErrorFiber(c)
			}
			toDelete = append(toDelete, id)
		}
		if len(toDelete) > 0 {
			if err := database.BulkDeleteObjects(c.Context(), toDelete); err != nil {
				return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "failed to delete records", err)
			}
		}

		count := len(toDelete)
		return c.JSON(internalapi.DeleteByQueryResponse{Deleted: &count})
	}
}

// handleInternalList handles listing, primarily to support lookup by hash.
func handleInternalListFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		hash := c.Query("hash")
		hashType := c.Query("hash_type")

		if hash != "" {
			hashType, hash = common.ParseHashQuery(hash, hashType)

			objs, err := database.GetObjectsByChecksum(c.Context(), hash)
			if err != nil {
				return writeDBErrorFiber(c, err)
			}

			var records []models.InternalObject
			for _, o := range objs {
				if hashType != "" && !common.ObjectHasChecksumTypeAndValue(o, hashType, hash) {
					continue
				}
				records = append(records, o)
			}

			return c.JSON(map[string]any{"records": records})
		}
		scopePrefix, hasScope, err := parseScopeQueryFiber(c)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, err.Error(), err)
		}
		limit, page, err := parseListPaginationFiber(c)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, err.Error(), err)
		}
		offset := page * limit
		if hasScope {
			if authz.IsGen3Mode(c.Context()) && !authz.HasAuthHeader(c.Context()) {
				return writeAuthErrorFiber(c)
			}
			ids, err := database.ListObjectIDsByResourcePrefix(c.Context(), scopePrefix)
			if err != nil {
				return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "Error listing records", err)
			}
			records := make([]models.InternalObject, 0, len(ids))
			for _, id := range ids {
				obj, err := database.GetObject(c.Context(), id)
				if err != nil {
					if errors.Is(err, common.ErrUnauthorized) || errors.Is(err, common.ErrNotFound) {
						continue
					}
					return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "Error fetching object", err)
				}
				if len(obj.Authorizations) > 0 && !authz.HasMethodAccess(c.Context(), "read", obj.Authorizations) {
					continue
				}
				records = append(records, *obj)
			}
			records = paginateRecords(records, offset, limit)
			return c.JSON(map[string]any{"records": records})
		}
		if authz.IsGen3Mode(c.Context()) && !authz.HasAuthHeader(c.Context()) {
			return writeAuthErrorFiber(c)
		}
		// Unscoped list
		ids, err := database.ListObjectIDsByResourcePrefix(c.Context(), "/")
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "Error listing records", err)
		}
		records := make([]models.InternalObject, 0, len(ids))
		for _, id := range ids {
			obj, err := database.GetObject(c.Context(), id)
			if err != nil {
				if errors.Is(err, common.ErrUnauthorized) || errors.Is(err, common.ErrNotFound) {
					continue
				}
				return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "Error fetching object", err)
			}
			if len(obj.Authorizations) > 0 && !authz.HasMethodAccess(c.Context(), "read", obj.Authorizations) {
				continue
			}
			records = append(records, *obj)
		}
		records = paginateRecords(records, offset, limit)
		return c.JSON(map[string]any{"records": records})
	}
}

func parseListPaginationFiber(c fiber.Ctx) (int, int, error) {
	const (
		defaultLimit = 50
		maxLimit     = 1000
	)
	limit := defaultLimit
	page := 0
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return 0, 0, fmt.Errorf("limit must be a positive integer")
		}
		if v > maxLimit {
			v = maxLimit
		}
		limit = v
	}
	if raw := strings.TrimSpace(c.Query("page")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return 0, 0, fmt.Errorf("page must be a non-negative integer")
		}
		page = v
	}
	return limit, page, nil
}

func paginateRecords(records []models.InternalObject, offset, limit int) []models.InternalObject {
	if offset < 0 {
		offset = 0
	}
	if limit < 0 {
		limit = 0
	}
	if offset >= len(records) {
		return []models.InternalObject{}
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return append([]models.InternalObject(nil), records[offset:end]...)
}
func handleInternalBulkDocumentsFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkDocumentsRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "invalid request body", err)
		}
		var dids []string
		if d0, err := req.AsBulkDocumentsRequest0(); err == nil {
			dids = d0
		} else if d1, err := req.AsBulkDocumentsRequest1(); err == nil {
			if d1.Dids != nil {
				dids = *d1.Dids
			} else if d1.Ids != nil {
				dids = *d1.Ids
			}
		}
		if len(dids) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "empty request", nil)
		}
		objs, err := database.GetBulkObjects(c.Context(), dids)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}
		out := make([]models.InternalObject, 0, len(objs))
		for i := range objs {
			if len(objs[i].Authorizations) > 0 && !authz.HasMethodAccess(c.Context(), "read", objs[i].Authorizations) {
				continue
			}
			out = append(out, objs[i])
		}
		return c.JSON(out)
	}
}

func handleInternalBulkDeleteHashesFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkHashesRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "invalid request body", err)
		}
		if len(req.Hashes) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "empty request", nil)
		}
		targetHashes := make([]string, len(req.Hashes))
		targetTypes := make([]string, len(req.Hashes))
		for i, h := range req.Hashes {
			targetTypes[i], targetHashes[i] = common.ParseHashQuery(h, "")
		}
		objsMap, err := database.GetObjectsByChecksums(c.Context(), targetHashes)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}
		toDelete := make([]string, 0)
		seen := make(map[string]struct{})
		for i := range targetHashes {
			hash := targetHashes[i]
			objs := objsMap[hash]
			for _, o := range objs {
				if targetTypes[i] != "" && !common.ObjectHasChecksumTypeAndValue(o, targetTypes[i], hash) {
					continue
				}
				if _, exists := seen[o.Id]; exists {
					continue
				}
				if !authz.HasMethodAccess(c.Context(), "delete", o.Authorizations) {
					continue
				}
				seen[o.Id] = struct{}{}
				toDelete = append(toDelete, o.Id)
			}
		}
		if len(toDelete) > 0 {
			if err := database.BulkDeleteObjects(c.Context(), toDelete); err != nil {
				return writeDBErrorFiber(c, err)
			}
		}
		count := len(toDelete)
		return c.JSON(internalapi.DeleteByQueryResponse{Deleted: &count})
	}
}

func handleInternalBulkSHA256ValidityFiber(database db.DatabaseInterface) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.BulkSHA256ValidityRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "invalid request body", err)
		}
		input := make([]string, 0)
		if req.Sha256 != nil {
			input = *req.Sha256
		} else if req.Hashes != nil {
			input = *req.Hashes
		}
		if len(input) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "empty request", nil)
		}
		resp, err := computeSHA256Validity(c.Context(), database, input)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}
		return c.JSON(resp)
	}
}

func computeSHA256Validity(ctx context.Context, database db.SHA256ValidityStore, values []string) (map[string]bool, error) {
	targets := common.NormalizeSHA256(values)
	if len(targets) == 0 {
		return nil, common.ErrNoValidSHA256
	}

	creds, err := database.ListS3Credentials(ctx)
	if err != nil {
		return nil, err
	}
	registeredBuckets := make(map[string]struct{}, len(creds))
	for _, c := range creds {
		if b := strings.TrimSpace(c.Bucket); b != "" {
			registeredBuckets[b] = struct{}{}
		}
	}

	objsMap, err := database.GetObjectsByChecksums(ctx, targets)
	if err != nil {
		return nil, err
	}

	resp := make(map[string]bool, len(targets))
	for _, sha := range targets {
		resp[sha] = false
		for _, obj := range objsMap[sha] {
			if hasValidRegisteredS3Target(obj, registeredBuckets) {
				resp[sha] = true
				break
			}
		}
	}

	return resp, nil
}

func hasValidRegisteredS3Target(obj models.InternalObject, registeredBuckets map[string]struct{}) bool {
	if obj.AccessMethods == nil {
		return false
	}
	for _, method := range *obj.AccessMethods {
		if !strings.EqualFold(string(method.Type), "s3") {
			continue
		}
		if method.AccessUrl == nil || method.AccessUrl.Url == "" {
			continue
		}
		bucket, key, ok := common.ParseS3URL(method.AccessUrl.Url)
		if !ok || key == "" {
			continue
		}
		if _, found := registeredBuckets[bucket]; !found {
			continue
		}
		return true
	}
	return false
}
