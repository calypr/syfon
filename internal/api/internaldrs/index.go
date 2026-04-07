package internaldrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	corelogic "github.com/calypr/syfon/internal/coreapi"
	"github.com/gorilla/mux"
)

// RegisterInternalIndexRoutes registers the Internal-compatible routes on the router.
func RegisterInternalIndexRoutes(router *mux.Router, database core.DatabaseInterface) {
	// Internal Endpoints
	router.Handle(config.RouteInternalIndex, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleInternalList(w, r, database)
		case http.MethodPost:
			handleInternalCreate(w, r, database)
		case http.MethodDelete:
			handleInternalDeleteByQuery(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "InternalIndex")).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)

	router.Handle(config.RouteInternalBulkHashes, drs.Logger(handleInternalBulkHashes(database), "InternalBulkHashes")).Methods(http.MethodPost)
	router.Handle(config.RouteInternalBulkDeleteHashes, drs.Logger(handleInternalBulkDeleteHashes(database), "InternalBulkDeleteHashes")).Methods(http.MethodPost)
	router.Handle(config.RouteInternalBulkSHA256, drs.Logger(handleInternalBulkSHA256Validity(database), "InternalBulkSHA256Validity")).Methods(http.MethodPost)
	router.Handle(config.RouteInternalMigrateBulk, drs.Logger(handleMigrateBulk(database), "InternalMigrateBulk")).Methods(http.MethodPost)
	router.Handle(config.RouteInternalBulkCreate, drs.Logger(handleInternalBulkCreate(database), "InternalBulkCreate")).Methods(http.MethodPost)
	router.Handle(config.RouteInternalBulkDocs, drs.Logger(handleInternalBulkDocuments(database), "InternalBulkDocuments")).Methods(http.MethodPost)

	router.Handle(config.RouteInternalIndexDetail, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleInternalGet(w, r, database)
		case http.MethodPut:
			handleInternalUpdate(w, r, database)
		case http.MethodDelete:
			handleInternalDelete(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "InternalDetail")).Methods(http.MethodGet, http.MethodPut, http.MethodDelete)
}

func handleInternalBulkCreate(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		if len(req.Records) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "records cannot be empty", nil)
			return
		}
		results := make([]internalapi.InternalRecordResponse, 0, len(req.Records))
		for i, rec := range req.Records {
			obj, err := internalToDrs(rec)
			if err != nil {
				writeHTTPError(w, r, http.StatusBadRequest, fmt.Sprintf("record[%d]: %v", i, err), err)
				return
			}
			targetResources := obj.Authorizations
			if len(targetResources) == 0 {
				targetResources = []string{"/data_file"}
				if !core.HasMethodAccess(r.Context(), "file_upload", targetResources) && !core.HasMethodAccess(r.Context(), "create", targetResources) {
					writeAuthError(w, r)
					return
				}
			} else if !core.HasMethodAccess(r.Context(), "create", targetResources) {
				writeAuthError(w, r)
				return
			}

			aliased, canonicalObj, aliasErr := maybeAliasBySHA256(r.Context(), database, rec, obj)
			if aliasErr != nil {
				writeDBError(w, r, aliasErr)
				return
			}
			if aliased {
				resp := drsToInternal(canonicalObj)
				resp.SetDid(obj.Id)
				results = append(results, resp)
				continue
			}
			if err := database.CreateObject(r.Context(), obj); err != nil {
				writeDBError(w, r, err)
				return
			}
			results = append(results, drsToInternal(obj))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(internalapi.ListRecordsResponse{Records: results}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

func handleInternalBulkDocuments(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		var dids []string
		if err := json.Unmarshal(body, &dids); err != nil {
			var wrapper struct {
				IDs  []string `json:"ids"`
				DIDs []string `json:"dids"`
			}
			if err2 := json.Unmarshal(body, &wrapper); err2 != nil {
				writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
				return
			}
			if len(wrapper.DIDs) > 0 {
				dids = wrapper.DIDs
			} else {
				dids = wrapper.IDs
			}
		}
		if len(dids) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "No ids provided", nil)
			return
		}
		objs, err := database.GetBulkObjects(r.Context(), dids)
		if err != nil {
			writeDBError(w, r, err)
			return
		}
		out := make([]internalapi.InternalRecordResponse, 0, len(objs))
		for i := range objs {
			if len(objs[i].Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", objs[i].Authorizations) {
				continue
			}
			out = append(out, drsToInternal(&objs[i]))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

// handleInternalBulkHashes allows looking up multiple records by their hashes.
func handleInternalBulkHashes(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkHashesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}

		// Normalize hashes while preserving optional type selectors.
		targetHashes := make([]string, len(req.Hashes))
		targetTypes := make([]string, len(req.Hashes))
		for i, h := range req.Hashes {
			targetTypes[i], targetHashes[i] = parseHashQuery(h, "")
		}

		objsMap, err := database.GetObjectsByChecksums(r.Context(), targetHashes)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		// Convert back to InternalRecord results
		// Gen3 Internal usually returns a mapping or a list. Let's return a list of records found.
		results := make([]internalapi.InternalRecordResponse, 0)
		seen := make(map[string]struct{})
		for i := range targetHashes {
			hash := targetHashes[i]
			objs := objsMap[hash]
			for _, o := range objs {
				if targetTypes[i] != "" && !objectHasChecksumTypeAndValue(o, targetTypes[i], hash) {
					continue
				}
				if len(o.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", o.Authorizations) {
					continue
				}
				if _, exists := seen[o.Id]; exists {
					continue
				}
				seen[o.Id] = struct{}{}
				results = append(results, drsToInternal(&o))
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(internalapi.ListRecordsResponse{Records: results}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

// handleInternalBulkDeleteHashes allows deleting multiple records by their hashes.
func handleInternalBulkDeleteHashes(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkHashesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		if len(req.Hashes) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "Hashes cannot be empty", nil)
			return
		}

		// Lookup records by their hashes to check permissions
		targetHashes := make([]string, len(req.Hashes))
		targetTypes := make([]string, len(req.Hashes))
		for i, h := range req.Hashes {
			targetTypes[i], targetHashes[i] = parseHashQuery(h, "")
		}

		objsMap, err := database.GetObjectsByChecksums(r.Context(), targetHashes)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		toDelete := make([]string, 0)
		seen := make(map[string]struct{})
		for i := range targetHashes {
			hash := targetHashes[i]
			objs := objsMap[hash]
			for _, o := range objs {
				if targetTypes[i] != "" && !objectHasChecksumTypeAndValue(o, targetTypes[i], hash) {
					continue
				}
				if _, exists := seen[o.Id]; exists {
					continue
				}
				// Check delete permissions
				targetResources := o.Authorizations
				if len(targetResources) == 0 {
					targetResources = []string{"/data_file"}
				}
				if !core.HasMethodAccess(r.Context(), "delete", targetResources) {
					continue
				}
				seen[o.Id] = struct{}{}
				toDelete = append(toDelete, o.Id)
			}
		}

		if len(toDelete) > 0 {
			if err := database.BulkDeleteObjects(r.Context(), toDelete); err != nil {
				writeDBError(w, r, err)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		count := int32(len(toDelete))
		if err := json.NewEncoder(w).Encode(internalapi.DeleteByQueryResponse{Deleted: &count}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

func handleInternalBulkSHA256Validity(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkSHA256ValidityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}

		input := req.Sha256
		if len(input) == 0 {
			input = req.Hashes
		}
		if len(input) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "No sha256 values provided", nil)
			return
		}

		resp, err := corelogic.ComputeSHA256Validity(r.Context(), database, input)
		if err != nil {
			if errors.Is(err, corelogic.ErrNoValidSHA256) {
				writeHTTPError(w, r, http.StatusBadRequest, "No valid sha256 values provided", nil)
				return
			}
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to compute sha256 validity: %v", err), err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

// handleInternalGet retrieves a record by DID.
func handleInternalGet(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	obj, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}

	record := drsToInternal(obj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(record); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalCreate creates a new record.
func handleInternalCreate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req internalapi.InternalRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	obj, err := internalToDrs(req)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	aliased, canonicalObj, aliasErr := maybeAliasBySHA256(r.Context(), database, req, obj)
	if aliasErr != nil {
		writeDBError(w, r, aliasErr)
		return
	}
	if aliased {
		response := drsToInternal(canonicalObj)
		response.SetDid(obj.Id)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	if err := database.CreateObject(r.Context(), obj); err != nil {
		writeDBError(w, r, err)
		return
	}

	response := drsToInternal(obj)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func maybeAliasBySHA256(ctx context.Context, database core.DatabaseInterface, req internalapi.InternalRecord, obj *core.InternalObject) (bool, *core.InternalObject, error) {
	if obj == nil {
		return false, nil, nil
	}
	sha := strings.TrimSpace(req.GetHashes()["sha256"])
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
func handleInternalUpdate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req internalapi.InternalRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}

	// Fetch existing first to check existence
	existing, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if req.GetDid() != "" && req.GetDid() != id {
		writeHTTPError(w, r, http.StatusBadRequest, "did cannot be changed", nil)
		return
	}

	updated := *existing
	updated.UpdatedTime = time.Now()
	updated.Id = id
	updated.SelfUri = "drs://" + id

	// Internal PUT typically sends full record payload. We treat present fields as replacements.
	updated.Size = req.GetSize()
	if req.GetFileName() != "" {
		updated.Name = req.GetFileName()
	}

	if req.HasUrls() {
		updated.AccessMethods = nil
		for _, u := range req.GetUrls() {
			updated.AccessMethods = append(updated.AccessMethods, drs.AccessMethod{
				Type:      "s3",
				AccessUrl: drs.AccessMethodAccessUrl{Url: u},
				Region:    "us-east-1",
			})
		}
	}

	if req.HasAuthz() {
		updated.Authorizations = append([]string(nil), req.GetAuthz()...)
	}
	if req.HasHashes() && len(req.GetHashes()) > 0 {
		updated.Checksums = nil
		for t, v := range req.GetHashes() {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: t, Checksum: v})
		}
		if len(updated.Checksums) == 0 {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
		}
	}

	if err := database.RegisterObjects(r.Context(), []core.InternalObject{
		updated,
	}); err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Failed to update object: %v", err), err)
		return
	}

	// Re-fetch to return latest state
	updatedObj, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}

	response := drsToInternal(updatedObj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalDelete deletes a record.
func handleInternalDelete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	if err := database.DeleteObject(r.Context(), id); err != nil {
		writeDBError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func parseScopeQuery(r *http.Request) (string, bool, error) {
	authz := strings.TrimSpace(r.URL.Query().Get("authz"))
	if authz != "" {
		return authz, true, nil
	}
	org := strings.TrimSpace(r.URL.Query().Get("organization"))
	if org == "" {
		org = strings.TrimSpace(r.URL.Query().Get("program"))
	}
	project := strings.TrimSpace(r.URL.Query().Get("project"))
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func handleInternalDeleteByQuery(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}

	hash := r.URL.Query().Get("hash")
	hashType := r.URL.Query().Get("hash_type")

	if !hasScope && hash == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "organization/project, authz, or hash query is required", nil)
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}

	var ids []string
	if hasScope {
		scopeIDs, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to list records by scope: %v", err), err)
			return
		}
		ids = append(ids, scopeIDs...)
	}

	if hash != "" {
		hashType, hash = parseHashQuery(hash, hashType)
		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			ids = append(ids, o.Id)
		}
	}

	toDelete := make([]string, 0, len(ids))
	for _, id := range ids {
		obj, err := database.GetObject(r.Context(), id)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				continue
			}
			writeDBError(w, r, err)
			return
		}
		targetResources := obj.Authorizations
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(r.Context(), "delete", targetResources) {
			writeAuthError(w, r)
			return
		}
		toDelete = append(toDelete, id)
	}
	if len(toDelete) > 0 {
		if err := database.BulkDeleteObjects(r.Context(), toDelete); err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to delete records: %v", err), err)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	count := int32(len(toDelete))
	if err := json.NewEncoder(w).Encode(internalapi.DeleteByQueryResponse{Deleted: &count}); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleInternalList handles listing, primarily to support lookup by hash.
func handleInternalList(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	// Query params: hash, hash_type
	hash := r.URL.Query().Get("hash")
	hashType := r.URL.Query().Get("hash_type")

	if hash != "" {
		hashType, hash = parseHashQuery(hash, hashType)

		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		var records []internalapi.InternalRecordResponse
		for _, o := range objs {
			if hashType != "" && !objectHasChecksumTypeAndValue(o, hashType, hash) {
				continue
			}
			records = append(records, drsToInternal(&o))
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	limit, page, err := parseListPagination(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	offset := page * limit
	if hasScope {
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error listing records: %v", err), err)
			return
		}
		records := make([]internalapi.InternalRecordResponse, 0, len(ids))
		for _, id := range ids {
			obj, err := database.GetObject(r.Context(), id)
			if err != nil {
				if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
					continue
				}
				writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error fetching object %s: %v", id, err), err)
				return
			}
			if len(obj.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
				continue
			}
			records = append(records, drsToInternal(obj))
		}
		records = paginateRecords(records, offset, limit)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}
	// Unscoped list: use root resource prefix to include all scoped records.
	ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), "/")
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error listing records: %v", err), err)
		return
	}
	records := make([]internalapi.InternalRecordResponse, 0, len(ids))
	for _, id := range ids {
		obj, err := database.GetObject(r.Context(), id)
		if err != nil {
			if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
				continue
			}
			writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("Error fetching object %s: %v", id, err), err)
			return
		}
		if len(obj.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
			continue
		}
		records = append(records, drsToInternal(obj))
	}
	records = paginateRecords(records, offset, limit)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func parseListPagination(r *http.Request) (int, int, error) {
	const (
		defaultLimit = 50
		maxLimit     = 1000
	)
	limit := defaultLimit
	page := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			return 0, 0, fmt.Errorf("limit must be a positive integer")
		}
		if v > maxLimit {
			v = maxLimit
		}
		limit = v
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("page")); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v < 0 {
			return 0, 0, fmt.Errorf("page must be a non-negative integer")
		}
		page = v
	}
	return limit, page, nil
}

func paginateRecords(records []internalapi.InternalRecordResponse, offset, limit int) []internalapi.InternalRecordResponse {
	if offset >= len(records) {
		return []internalapi.InternalRecordResponse{}
	}
	end := offset + limit
	if end > len(records) {
		end = len(records)
	}
	return records[offset:end]
}
