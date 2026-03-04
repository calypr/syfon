package gen3

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/gorilla/mux"
)

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		code := http.StatusForbidden
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			code = http.StatusUnauthorized
		}
		http.Error(w, "Unauthorized", code)
	case errors.Is(err, core.ErrNotFound):
		http.Error(w, "Object not found", http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// IndexdRecord represents the JSON structure of a Gen3 Indexd record.
// This is a simplified version tailored to what git-drs expects.
type IndexdRecord struct {
	DID          string                 `json:"did"`
	BaseID       string                 `json:"baseid,omitempty"`
	Rev          string                 `json:"rev,omitempty"`
	Size         int64                  `json:"size"`
	Hashes       map[string]string      `json:"hashes"`
	URLs         []string               `json:"urls"`
	ACL          []string               `json:"acl"`
	Authz        []string               `json:"authz"`
	Organization string                 `json:"organization,omitempty"`
	Project      string                 `json:"project,omitempty"`
	FileName     string                 `json:"file_name,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	CreatedDate  string                 `json:"created_date,omitempty"`
	UpdatedDate  string                 `json:"updated_date,omitempty"`
	Version      string                 `json:"version,omitempty"`
	Uploader     string                 `json:"uploader,omitempty"`
}

// ListRecordsResponse represents the wrapper for listing records in Indexd.
type ListRecordsResponse struct {
	Records []IndexdRecord `json:"records"`
}

type IndexdBulkCreateRequest struct {
	Records []IndexdRecord `json:"records"`
}

// RegisterGen3Routes registers the Indexd-compatible routes on the router.
func RegisterGen3Routes(router *mux.Router, database core.DatabaseInterface) {
	// Indexd Endpoints
	router.Handle("/index/index", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleIndexdList(w, r, database)
		case http.MethodPost:
			handleIndexdCreate(w, r, database)
		case http.MethodDelete:
			handleIndexdDeleteByQuery(w, r, database)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}), "IndexdIndex")).Methods(http.MethodGet, http.MethodPost, http.MethodDelete)

	router.Handle("/index/index/bulk/hashes", drs.Logger(handleIndexdBulkHashes(database), "IndexdBulkHashes")).Methods(http.MethodPost)
	router.Handle("/index/index/bulk", drs.Logger(handleIndexdBulkCreate(database), "IndexdBulkCreate")).Methods(http.MethodPost)
	router.Handle("/bulk/documents", drs.Logger(handleIndexdBulkDocuments(database), "IndexdBulkDocuments")).Methods(http.MethodPost)

	router.Handle("/index/index/{id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleIndexdGet(w, r, database)
		case http.MethodPut:
			handleIndexdUpdate(w, r, database)
		case http.MethodDelete:
			handleIndexdDelete(w, r, database)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}), "IndexdDetail")).Methods(http.MethodGet, http.MethodPut, http.MethodDelete)
}

func canonicalIDFromIndexd(req IndexdRecord) string {
	if req.DID != "" {
		return req.DID
	}
	if v := req.Hashes["sha256"]; v != "" {
		return v
	}
	if v := req.Hashes["sha-256"]; v != "" {
		return v
	}
	return ""
}

func indexdToDrs(req IndexdRecord) (*drs.DrsObject, []string, error) {
	id := canonicalIDFromIndexd(req)
	if id == "" {
		return nil, nil, fmt.Errorf("did or sha256 hash is required")
	}
	now := time.Now()
	obj := &drs.DrsObject{
		Id:             id,
		SelfUri:        "drs://" + id,
		Size:           req.Size,
		CreatedTime:    now,
		UpdatedTime:    now,
		Name:           req.FileName,
		Authorizations: append([]string(nil), req.Authz...),
	}
	for t, v := range req.Hashes {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}
	if len(obj.Checksums) == 0 {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
	}
	for _, u := range req.URLs {
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			Type:      "s3",
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Region:    "us-east-1",
		})
	}
	authz := append([]string(nil), req.Authz...)
	if len(authz) == 0 && req.Organization != "" {
		path := core.ResourcePathForScope(req.Organization, req.Project)
		if path != "" {
			authz = append(authz, path)
		}
	}
	obj.Authorizations = append([]string(nil), authz...)
	for i := range obj.AccessMethods {
		obj.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
			BearerAuthIssuers: authz,
		}
	}
	return obj, authz, nil
}

func handleIndexdBulkCreate(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req IndexdBulkCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		if len(req.Records) == 0 {
			http.Error(w, "records cannot be empty", http.StatusBadRequest)
			return
		}
		results := make([]IndexdRecord, 0, len(req.Records))
		for i, rec := range req.Records {
			obj, authz, err := indexdToDrs(rec)
			if err != nil {
				http.Error(w, fmt.Sprintf("record[%d]: %v", i, err), http.StatusBadRequest)
				return
			}
			targetResources := authz
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

			if err := database.CreateObject(r.Context(), obj, authz); err != nil {
				http.Error(w, fmt.Sprintf("record[%d]: %v", i, err), http.StatusInternalServerError)
				return
			}
			results = append(results, drsToIndexd(obj))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(ListRecordsResponse{Records: results})
	}
}

func handleIndexdBulkDocuments(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		var dids []string
		if err := json.Unmarshal(body, &dids); err != nil {
			var wrapper struct {
				IDs  []string `json:"ids"`
				DIDs []string `json:"dids"`
			}
			if err2 := json.Unmarshal(body, &wrapper); err2 != nil {
				http.Error(w, "Invalid request body", http.StatusBadRequest)
				return
			}
			if len(wrapper.DIDs) > 0 {
				dids = wrapper.DIDs
			} else {
				dids = wrapper.IDs
			}
		}
		if len(dids) == 0 {
			http.Error(w, "No ids provided", http.StatusBadRequest)
			return
		}
		objs, err := database.GetBulkObjects(r.Context(), dids)
		if err != nil {
			writeDBError(w, r, err)
			return
		}
		out := make([]IndexdRecord, 0, len(objs))
		for i := range objs {
			if len(objs[i].Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", objs[i].Authorizations) {
				continue
			}
			out = append(out, drsToIndexd(&objs[i]))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

// handleIndexdBulkHashes allows looking up multiple records by their hashes.
func handleIndexdBulkHashes(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Hashes []string `json:"hashes"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		// Normalize hashes
		targetHashes := make([]string, len(req.Hashes))
		for i, h := range req.Hashes {
			if parts := strings.SplitN(h, ":", 2); len(parts) == 2 {
				targetHashes[i] = parts[1]
			} else {
				targetHashes[i] = h
			}
		}

		objsMap, err := database.GetObjectsByChecksums(r.Context(), targetHashes)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		// Convert back to IndexdRecord results
		// Gen3 Indexd usually returns a mapping or a list. Let's return a list of records found.
		results := make([]IndexdRecord, 0)
		for _, objs := range objsMap {
			for _, o := range objs {
				if len(o.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", o.Authorizations) {
					continue
				}
				results = append(results, drsToIndexd(&o))
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ListRecordsResponse{Records: results})
	}
}

// Helper to convert internal DrsObject to Gen3 IndexdRecord
func drsToIndexd(obj *drs.DrsObject) IndexdRecord {
	hashes := make(map[string]string)
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

	return IndexdRecord{
		DID:          obj.Id,
		Size:         obj.Size,
		Hashes:       hashes,
		URLs:         urls,
		Authz:        authz,
		Organization: core.ParseResourcePath(firstAuthz(authz)).Organization,
		Project:      core.ParseResourcePath(firstAuthz(authz)).Project,
		CreatedDate:  obj.CreatedTime.Format(time.RFC3339),
		UpdatedDate:  obj.UpdatedTime.Format(time.RFC3339),
		FileName:     obj.Name, // Using Name as file_name
	}
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
	json.NewEncoder(w).Encode(record)
}

// handleIndexdCreate creates a new record.
func handleIndexdCreate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req IndexdRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	obj, authz, err := indexdToDrs(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := database.CreateObject(r.Context(), obj, authz); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create object: %v", err), http.StatusInternalServerError)
		return
	}

	response := drsToIndexd(obj)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated) // 201
	json.NewEncoder(w).Encode(response)
}

// handleIndexdUpdate updates an existing record.
func handleIndexdUpdate(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	var req IndexdRecord
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Fetch existing first to check existence
	existing, err := database.GetObject(r.Context(), id)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if req.DID != "" && req.DID != id {
		http.Error(w, "did cannot be changed", http.StatusBadRequest)
		return
	}

	updated := *existing
	updated.UpdatedTime = time.Now()
	updated.Id = id
	updated.SelfUri = "drs://" + id

	// Indexd PUT typically sends full record payload. We treat present fields as replacements.
	updated.Size = req.Size
	if req.FileName != "" {
		updated.Name = req.FileName
	}

	if req.URLs != nil {
		updated.AccessMethods = nil
		for _, u := range req.URLs {
			updated.AccessMethods = append(updated.AccessMethods, drs.AccessMethod{
				Type:      "s3",
				AccessUrl: drs.AccessMethodAccessUrl{Url: u},
				Region:    "us-east-1",
			})
		}
	}

	authz := existing.Authorizations
	if req.Authz != nil {
		authz = append([]string(nil), req.Authz...)
		updated.Authorizations = append([]string(nil), req.Authz...)
	}
	if req.Hashes != nil {
		updated.Checksums = nil
		for t, v := range req.Hashes {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: t, Checksum: v})
		}
		if len(updated.Checksums) == 0 {
			updated.Checksums = append(updated.Checksums, drs.Checksum{Type: "sha256", Checksum: id})
		}
	}

	if err := database.RegisterObjects(r.Context(), []core.DrsObjectWithAuthz{
		{DrsObject: updated, Authz: authz},
	}); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update object: %v", err), http.StatusInternalServerError)
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
	json.NewEncoder(w).Encode(response)
}

// handleIndexdDelete deletes a record.
func handleIndexdDelete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	if err := database.DeleteObject(r.Context(), id); err != nil {
		// If delete fails, it might be not found, or other error.
		http.Error(w, fmt.Sprintf("Failed to delete object: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	http.Error(w, "Unauthorized", code)
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !hasScope {
		http.Error(w, "organization/project or authz query is required", http.StatusBadRequest)
		return
	}
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		writeAuthError(w, r)
		return
	}

	ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to list records: %v", err), http.StatusInternalServerError)
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
			http.Error(w, fmt.Sprintf("failed to delete records: %v", err), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"deleted": len(toDelete)})
}

// handleIndexdList handles listing, primarily to support lookup by hash.
func handleIndexdList(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	// Query params: hash, hash_type
	hash := r.URL.Query().Get("hash")
	// hash_type := r.URL.Query().Get("hash_type") // We can assume sha256 or iterate, current db method only takes value (naive)

	if hash != "" {
		// Parse "TYPE:VALUE" if present (e.g. from IndexdClient)
		if parts := strings.SplitN(hash, ":", 2); len(parts) == 2 {
			hash = parts[1]
		}

		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			writeDBError(w, r, err)
			return
		}

		var records []IndexdRecord
		for _, o := range objs {
			records = append(records, drsToIndexd(&o))
		}

		response := ListRecordsResponse{
			Records: records,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}
	scopePrefix, hasScope, err := parseScopeQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if hasScope {
		if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		ids, err := database.ListObjectIDsByResourcePrefix(r.Context(), scopePrefix)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error listing records: %v", err), http.StatusInternalServerError)
			return
		}
		records := make([]IndexdRecord, 0, len(ids))
		for _, id := range ids {
			obj, err := database.GetObject(r.Context(), id)
			if err != nil {
				if errors.Is(err, core.ErrUnauthorized) || errors.Is(err, core.ErrNotFound) {
					continue
				}
				http.Error(w, fmt.Sprintf("Error fetching object %s: %v", id, err), http.StatusInternalServerError)
				return
			}
			if len(obj.Authorizations) > 0 && !core.HasMethodAccess(r.Context(), "read", obj.Authorizations) {
				continue
			}
			records = append(records, drsToIndexd(obj))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ListRecordsResponse{Records: records})
		return
	}

	// If no hash, maybe valid list?
	// Not strictly required for the test case described (which uses GetObjectByHash), but good to return empty list or not implemented.
	http.Error(w, "Listing not fully implemented without query params", http.StatusNotImplemented)
}
