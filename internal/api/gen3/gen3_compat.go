package gen3

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/gorilla/mux"
)

// IndexdRecord represents the JSON structure of a Gen3 Indexd record.
// This is a simplified version tailored to what git-drs expects.
type IndexdRecord struct {
	DID         string                 `json:"did"`
	BaseID      string                 `json:"baseid,omitempty"`
	Rev         string                 `json:"rev,omitempty"`
	Size        int64                  `json:"size"`
	Hashes      map[string]string      `json:"hashes"`
	URLs        []string               `json:"urls"`
	ACL         []string               `json:"acl"`
	Authz       []string               `json:"authz"`
	FileName    string                 `json:"file_name,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	CreatedDate string                 `json:"created_date,omitempty"`
	UpdatedDate string                 `json:"updated_date,omitempty"`
	Version     string                 `json:"version,omitempty"`
	Uploader    string                 `json:"uploader,omitempty"`
}

// RegisterGen3Routes registers the Indexd-compatible routes on the router.
func RegisterGen3Routes(router *mux.Router, database core.DatabaseInterface) {
	// Indexd Endpoints
	router.HandleFunc("/index/index", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleIndexdList(w, r, database)
		case http.MethodPost:
			handleIndexdCreate(w, r, database)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodGet, http.MethodPost)

	router.HandleFunc("/index/index/{id}", func(w http.ResponseWriter, r *http.Request) {
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
	}).Methods(http.MethodGet, http.MethodPut, http.MethodDelete)
}

// Helper to convert internal DrsObject to Gen3 IndexdRecord
func drsToIndexd(obj *drs.DrsObject) IndexdRecord {
	hashes := make(map[string]string)
	for _, c := range obj.Checksums {
		hashes[c.Type] = c.Checksum
	}

	var urls []string
	var authz []string
	if len(obj.AccessMethods) > 0 {
		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url != "" {
				urls = append(urls, am.AccessUrl.Url)
			}
			// Workaround: Store authz in BearerAuthIssuers[0]
			if len(am.Authorizations.BearerAuthIssuers) > 0 {
				val := am.Authorizations.BearerAuthIssuers[0]
				if val != "" {
					// Avoid duplicates
					found := false
					for _, a := range authz {
						if a == val {
							found = true
							break
						}
					}
					if !found {
						authz = append(authz, val)
					}
				}
			}
		}
	}

	return IndexdRecord{
		DID:         obj.Id,
		Size:        obj.Size,
		Hashes:      hashes,
		URLs:        urls,
		Authz:       authz,
		CreatedDate: obj.CreatedTime.Format(time.RFC3339),
		UpdatedDate: obj.UpdatedTime.Format(time.RFC3339),
		FileName:    obj.Name, // Using Name as file_name
	}
}

// handleIndexdGet retrieves a record by DID.
func handleIndexdGet(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	id := vars["id"]

	obj, err := database.GetObject(r.Context(), id)
	if err != nil {
		// Assume 404 if error, though could be 500
		http.Error(w, fmt.Sprintf("Object not found: %v", err), http.StatusNotFound)
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

	// Map to DrsObject
	now := time.Now()
	obj := drs.DrsObject{
		Id:          req.DID, // User provided ID (often UUID based on hash)
		SelfUri:     "drs://generated/" + req.DID,
		Size:        req.Size,
		CreatedTime: now,
		UpdatedTime: now,
		Name:        req.FileName,
	}

	// Checksums
	for t, v := range req.Hashes {
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}

	// Access Methods (URLs)
	// We map the URLs to access methods. We assume a default type/cloud/region if not provided.
	for _, u := range req.URLs {
		am := drs.AccessMethod{
			Type:      "s3", // Default assumption for git-drs usage
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Region:    "us-east-1", // Default
		}

		// Map Authz to AccessMethod Authorizations as this is how we store project info
		// git-drs passes authz list. We attach it to the access method so it persists.
		if len(req.Authz) > 0 {
			// Store in BearerAuthIssuers as workaround
			am.Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: []string{req.Authz[0]},
			}
		}
		obj.AccessMethods = append(obj.AccessMethods, am)
	}

	if len(req.URLs) == 0 && len(req.Authz) > 0 {
		// Create a placeholder AM to store Authz?
		// git-drs often registers metadata-only first.
		// We'll create a dummy "https" access method with no URL to carry the authz info.
		am := drs.AccessMethod{
			Type: "https",
			// No URL
			Authorizations: drs.AccessMethodAuthorizations{
				BearerAuthIssuers: []string{req.Authz[0]},
			},
		}
		obj.AccessMethods = append(obj.AccessMethods, am)
	}

	if err := database.CreateObject(r.Context(), &obj); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create object: %v", err), http.StatusInternalServerError)
		return
	}

	response := drsToIndexd(&obj)
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
	obj, err := database.GetObject(r.Context(), id)
	if err != nil {
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}

	// git-drs mainly uses this to ADD URLs (via `AddURL` -> `upsertIndexdRecord` which calls `UpdateRecord`).
	// It sends the FULL record usually.

	// We should overwrite URLs (AccessMethods) with what's in the request.

	var newAccessMethods []drs.AccessMethod
	for _, u := range req.URLs {
		am := drs.AccessMethod{
			Type:      "s3",
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Region:    "us-east-1",
		}
		if len(req.Authz) > 0 {
			am.Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: []string{req.Authz[0]},
			}
		} else if len(obj.AccessMethods) > 0 && len(obj.AccessMethods[0].Authorizations.BearerAuthIssuers) > 0 {
			// Preserve existing authz
			am.Authorizations = obj.AccessMethods[0].Authorizations
		}
		newAccessMethods = append(newAccessMethods, am)
	}

	// Update DB
	if err := database.UpdateObjectAccessMethods(r.Context(), id, newAccessMethods); err != nil {
		http.Error(w, fmt.Sprintf("Failed to update access methods: %v", err), http.StatusInternalServerError)
		return
	}

	// Re-fetch to return latest state
	updatedObj, err := database.GetObject(r.Context(), id)
	if err != nil {
		http.Error(w, "Failed to fetch updated object", http.StatusInternalServerError)
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

// handleIndexdList handles listing, primarily to support lookup by hash.
func handleIndexdList(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	// Query params: hash, hash_type
	hash := r.URL.Query().Get("hash")
	// hash_type := r.URL.Query().Get("hash_type") // We can assume sha256 or iterate, current db method only takes value (naive)

	if hash != "" {
		// This is a lookup by hash
		// Note: The DB interface `GetObjectsByChecksum` expects just the checksum value assuming uniqueness or similar?
		// Wait, different checksum types might collide? Ideally we should filter by type too, but for now we trust the DB to find matching checksums.

		objs, err := database.GetObjectsByChecksum(r.Context(), hash)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error looking up by hash: %v", err), http.StatusInternalServerError)
			return
		}

		var records []IndexdRecord
		for _, o := range objs {
			records = append(records, drsToIndexd(&o))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(records)
		return
	}

	// If no hash, maybe valid list?
	// Not strictly required for the test case described (which uses GetObjectByHash), but good to return empty list or not implemented.
	http.Error(w, "Listing not fully implemented without query params", http.StatusNotImplemented)
}
