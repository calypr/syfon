package admin

import (
	"encoding/json"
	"net/http"

	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
)

func RegisterAdminRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	// Credentials API
	router.HandleFunc("/admin/credentials", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			listCredentials(w, r, database)
		case http.MethodPut:
			putCredential(w, r, database)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodGet, http.MethodPut)

	router.HandleFunc("/admin/credentials/{bucket}", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			deleteCredential(w, r, database)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}).Methods(http.MethodDelete)

	// Utility endpoint for signing URLs
	router.HandleFunc("/admin/sign_url", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		signURLHandler(w, r, uM)
	}).Methods(http.MethodPost)
}

func listCredentials(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(creds)
}

func putCredential(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	var req struct {
		Bucket          string `json:"bucket"`
		BucketLegacy    string `json:"Bucket"`
		Region          string `json:"region"`
		RegionLegacy    string `json:"Region"`
		AccessKey       string `json:"access_key"`
		AccessKeyLegacy string `json:"AccessKey"`
		SecretKey       string `json:"secret_key"`
		SecretKeyLegacy string `json:"SecretKey"`
		Endpoint        string `json:"endpoint"`
		EndpointLegacy  string `json:"Endpoint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	cred := core.S3Credential{
		Bucket:    firstNonEmpty(req.Bucket, req.BucketLegacy),
		Region:    firstNonEmpty(req.Region, req.RegionLegacy),
		AccessKey: firstNonEmpty(req.AccessKey, req.AccessKeyLegacy),
		SecretKey: firstNonEmpty(req.SecretKey, req.SecretKeyLegacy),
		Endpoint:  firstNonEmpty(req.Endpoint, req.EndpointLegacy),
	}
	// Basic validation
	if cred.Bucket == "" || cred.AccessKey == "" || cred.SecretKey == "" {
		http.Error(w, "Missing required fields (bucket, access_key, secret_key)", http.StatusBadRequest)
		return
	}

	if err := database.SaveS3Credential(r.Context(), &cred); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func deleteCredential(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	if bucket == "" {
		http.Error(w, "Bucket name required", http.StatusBadRequest)
		return
	}

	if err := database.DeleteS3Credential(r.Context(), bucket); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func signURLHandler(w http.ResponseWriter, r *http.Request, uM urlmanager.UrlManager) {
	var req struct {
		URL    string `json:"url"`    // s3://bucket/key
		Method string `json:"method"` // GET or PUT
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	var signedURL string
	var err error

	if req.Method == "PUT" {
		signedURL, err = uM.SignUploadURL(r.Context(), "", req.URL, urlmanager.SignOptions{})
	} else {
		signedURL, err = uM.SignURL(r.Context(), "", req.URL, urlmanager.SignOptions{})
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest) // 400 because maybe invalid URL
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"signed_url": signedURL})
}
