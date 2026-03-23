package gen3

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/internalapi"
	"github.com/calypr/drs-server/db/core"
	corelogic "github.com/calypr/drs-server/internal/coreapi"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := core.GetRequestID(r.Context())
	if err != nil {
		slog.Error("gen3 request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("gen3 request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		code := http.StatusForbidden
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			code = http.StatusUnauthorized
		}
		writeHTTPError(w, r, code, "Unauthorized", err)
	case errors.Is(err, core.ErrNotFound):
		writeHTTPError(w, r, http.StatusNotFound, "Object not found", err)
	default:
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
	}
}

// RegisterGen3Routes registers the Indexd-compatible routes on the router.
func RegisterGen3Routes(router *mux.Router, database core.DatabaseInterface) {
	// Indexd Endpoints
	router.Handle("/index", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleIndexdList(w, r, database)
		case http.MethodPost:
			handleIndexdCreate(w, r, database)
		case http.MethodDelete:
			handleIndexdDeleteByQuery(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "IndexdIndex")).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)

	router.Handle("/index/bulk/hashes", drs.Logger(handleIndexdBulkHashes(database), "IndexdBulkHashes")).Methods(http.MethodPost)
	router.Handle("/index/bulk/sha256/validity", drs.Logger(handleIndexdBulkSHA256Validity(database), "IndexdBulkSHA256Validity")).Methods(http.MethodPost)
	router.Handle("/index/bulk", drs.Logger(handleIndexdBulkCreate(database), "IndexdBulkCreate")).Methods(http.MethodPost)
	router.Handle("/index/bulk/documents", drs.Logger(handleIndexdBulkDocuments(database), "IndexdBulkDocuments")).Methods(http.MethodPost)

	router.Handle("/index/{id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleIndexdGet(w, r, database)
		case http.MethodPut:
			handleIndexdUpdate(w, r, database)
		case http.MethodDelete:
			handleIndexdDelete(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "IndexdDetail")).Methods(http.MethodGet, http.MethodPut, http.MethodDelete)
}

func canonicalIDFromIndexd(req internalapi.IndexdRecord) string {
	if did := strings.TrimSpace(req.GetDid()); did != "" {
		if _, err := uuid.Parse(did); err == nil {
			return did
		}
	}
	hashes := req.GetHashes()
	if v, ok := hashes["sha256"]; ok && strings.TrimSpace(v) != "" {
		authz := append([]string(nil), req.GetAuthz()...)
		if len(authz) == 0 && req.HasOrganization() {
			path := core.ResourcePathForScope(req.GetOrganization(), req.GetProject())
			if path != "" {
				authz = append(authz, path)
			}
		}
		return core.MintObjectIDFromChecksum(v, authz)
	}
	return ""
}

func indexdToDrs(req internalapi.IndexdRecord) (*core.InternalObject, error) {
	id := canonicalIDFromIndexd(req)
	if id == "" {
		return nil, fmt.Errorf("sha256 hash is required unless did is a UUID")
	}
	now := time.Now()
	obj := &drs.DrsObject{
		Id:          id,
		SelfUri:     "drs://" + id,
		Size:        req.GetSize(),
		CreatedTime: now,
		UpdatedTime: now,
		Name:        req.GetFileName(),
	}
	for t, v := range req.GetHashes() {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}
	if len(obj.Checksums) == 0 {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
	}
	for _, u := range req.GetUrls() {
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			Type:      "s3",
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Region:    "us-east-1",
		})
	}
	authz := append([]string(nil), req.GetAuthz()...)
	if len(authz) == 0 && req.HasOrganization() {
		path := core.ResourcePathForScope(req.GetOrganization(), req.GetProject())
		if path != "" {
			authz = append(authz, path)
		}
	}
	for i := range obj.AccessMethods {
		obj.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
			BearerAuthIssuers: authz,
		}
	}
	return &core.InternalObject{DrsObject: *obj, Authorizations: authz}, nil
}

func handleIndexdBulkCreate(database core.DatabaseInterface) http.HandlerFunc {
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
		results := make([]internalapi.IndexdRecordResponse, 0, len(req.Records))
		for i, rec := range req.Records {
			obj, err := indexdToDrs(rec)
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

			if err := database.CreateObject(r.Context(), obj); err != nil {
				writeDBError(w, r, err)
				return
			}
			results = append(results, drsToIndexd(obj))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(internalapi.ListRecordsResponse{Records: results}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

func handleIndexdBulkDocuments(database core.DatabaseInterface) http.HandlerFunc {
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
		out := make([]internalapi.IndexdRecordResponse, 0, len(objs))
		for i := range objs {
			if len(objs[i].Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", objs[i].Authorizations) {
				continue
			}
			out = append(out, drsToIndexd(&objs[i]))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

// handleIndexdBulkHashes allows looking up multiple records by their hashes.
func handleIndexdBulkHashes(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkHashesRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
			return
		}

		// Normalize hashes
		targetHashes := make([]string, len(req.Hashes))
		for i, h := range req.Hashes {
			targetHashes[i] = normalizeHashQueryValue(h)
		}

		objsMap, err := database.GetObjectsByChecksums(r.Context(), targetHashes)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		// Convert back to IndexdRecord results
		// Gen3 Indexd usually returns a mapping or a list. Let's return a list of records found.
		results := make([]internalapi.IndexdRecordResponse, 0)
		for _, objs := range objsMap {
			for _, o := range objs {
				if len(o.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", o.Authorizations) {
					continue
				}
				results = append(results, drsToIndexd(&o))
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(internalapi.ListRecordsResponse{Records: results}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}

func handleIndexdBulkSHA256Validity(database core.DatabaseInterface) http.HandlerFunc {
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

// Helper to convert internal DrsObject to Gen3 IndexdRecord
func drsToIndexd(obj *core.InternalObject) internalapi.IndexdRecordResponse {
	hashes := make(map[string]string, len(obj.Checksums))
	for _, c := range obj.Checksums {
		hashes[c.Type] = c.Checksum
	}
	if len(hashes) == 0 && obj.Id != "" {
		hashes["sha256"] = obj.Id
	}

	var urls []string
	authz := append([]string(nil), obj.Authorizations...)
	if len(obj.AccessMethods) > 0 {
		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url != "" {
				urls = append(urls, am.AccessUrl.Url)
			}
		}
	}
	scope := core.ParseResourcePath(firstAuthz(authz))
	resp := internalapi.IndexdRecordResponse{
		Authz: authz,
		Urls:  urls,
	}
	if obj.Id != "" {
		resp.SetDid(obj.Id)
	}
	resp.SetSize(obj.Size)
	if len(hashes) > 0 {
		resp.SetHashes(hashes)
	}
	if obj.Name != "" {
		resp.SetFileName(obj.Name)
	}
	if scope.Organization != "" {
		resp.SetOrganization(scope.Organization)
	}
	if scope.Project != "" {
		resp.SetProject(scope.Project)
	}
	if !obj.CreatedTime.IsZero() {
		resp.SetCreatedDate(obj.CreatedTime.Format(time.RFC3339))
	}
	if !obj.UpdatedTime.IsZero() {
		resp.SetUpdatedDate(obj.UpdatedTime.Format(time.RFC3339))
	}
	return resp
}

func firstAuthz(authz []string) string {
	if len(authz) == 0 {
		return ""
	}
	return authz[0]
}

// handleIndexdGet retrieves a record by DID.
func handleIndexdGet(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	obj, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}

	record := drsToIndexd(obj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(record); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleIndexdCreate creates a new record.
func handleIndexdCreate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req internalapi.IndexdRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	obj, err := indexdToDrs(req)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	if err := database.CreateObject(r.Context(), obj); err != nil {
		writeDBError(w, r, err)
		return
	}

	response := drsToIndexd(obj)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleIndexdUpdate updates an existing record.
func handleIndexdUpdate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req internalapi.IndexdRecord
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

	// Indexd PUT typically sends full record payload. We treat present fields as replacements.
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

	response := drsToIndexd(updatedObj)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleIndexdDelete deletes a record.
func handleIndexdDelete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	if err := database.DeleteObject(r.Context(), id); err != nil {
		writeDBError(w, r, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	writeHTTPError(w, r, code, "Unauthorized", nil)
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

func handleIndexdDeleteByQuery(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	if !hasScope {
		writeHTTPError(w, r, http.StatusBadRequest, "organization/project or authz query is required", nil)
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}

	ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, fmt.Sprintf("failed to list records: %v", err), err)
		return
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
	if err := json.NewEncoder(w).Encode(map[string]int{"deleted": len(toDelete)}); err != nil {
		slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

// handleIndexdList handles listing, primarily to support lookup by hash.
func handleIndexdList(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	// Query params: hash, hash_type
	hash := r.URL.Query().Get("hash")
	// hash_type := r.URL.Query().Get("hash_type") // We can assume sha256 or iterate, current db method only takes value (naive)

	if hash != "" {
		hash = normalizeHashQueryValue(hash)

		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		var records []internalapi.IndexdRecordResponse
		for _, o := range objs {
			records = append(records, drsToIndexd(&o))
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
		records := make([]internalapi.IndexdRecordResponse, 0, len(ids))
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
			records = append(records, drsToIndexd(obj))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"records": records}); err != nil {
			slog.Error("gen3 encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
		return
	}

	// If no hash, maybe valid list?
	// Not strictly required for the test case described (which uses GetObjectByHash), but good to return empty list or not implemented.
	writeHTTPError(w, r, http.StatusNotImplemented, "Listing not fully implemented without query params", nil)
}

func normalizeHashQueryValue(raw string) string {
	clean := strings.Trim(strings.TrimSpace(raw), `"'`)
	if parts := strings.SplitN(clean, ":", 2); len(parts) == 2 {
		return strings.Trim(strings.TrimSpace(parts[1]), `"'`)
	}
	return clean
}
