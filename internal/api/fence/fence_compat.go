package fence

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type fenceSignedURL struct {
	URL string `json:"url"`
}

type fenceUploadBlankRequest struct {
	GUID  string   `json:"guid"`
	Authz []string `json:"authz"`
}

type fenceUploadBlankResponse struct {
	GUID string `json:"guid"`
	URL  string `json:"url"`
}

type fenceMultipartInitRequest struct {
	GUID     string `json:"guid"`
	FileName string `json:"file_name"`
	Bucket   string `json:"bucket"`
}

type fenceMultipartInitResponse struct {
	GUID     string `json:"guid"`
	UploadID string `json:"uploadId"`
}

type fenceMultipartUploadRequest struct {
	UploadID   string `json:"uploadId"`
	PartNumber int32  `json:"partNumber"`
}

type fenceMultipartUploadResponse struct {
	PresignedURL string `json:"presigned_url"`
}

type fenceMultipartPart struct {
	PartNumber int32  `json:"partNumber"`
	ETag       string `json:"etag"`
}

type fenceMultipartCompleteRequest struct {
	UploadID string               `json:"uploadId"`
	Parts    []fenceMultipartPart `json:"parts"`
}

type fenceBucketInfo struct {
	Name   string `json:"name"`
	Region string `json:"region"`
}

func RegisterFenceRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	// Download
	router.HandleFunc("/data/download/{file_id}", func(w http.ResponseWriter, r *http.Request) {
		handleFenceDownload(w, r, database, uM)
	}).Methods(http.MethodGet)

	// Upload
	router.HandleFunc("/data/upload", func(w http.ResponseWriter, r *http.Request) {
		handleFenceUploadBlank(w, r, database, uM)
	}).Methods(http.MethodPost)

	router.HandleFunc("/data/upload/{file_id}", func(w http.ResponseWriter, r *http.Request) {
		handleFenceUploadURL(w, r, database, uM)
	}).Methods(http.MethodGet)

	// Multipart
	router.HandleFunc("/multipart/init", func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartInit(w, r, database, uM)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart/upload", func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartUpload(w, r, database, uM)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart/complete", func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartComplete(w, r, database, uM)
	}).Methods(http.MethodPost)

	// Buckets
	router.HandleFunc("/data/buckets", func(w http.ResponseWriter, r *http.Request) {
		handleFenceBuckets(w, r, database)
	}).Methods(http.MethodGet)
}

func handleFenceDownload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	obj, err := database.GetObject(r.Context(), fileID)
	if err != nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	// Find S3 access method
	var s3URL string
	for _, am := range obj.AccessMethods {
		if am.Type == "s3" && am.AccessUrl.Url != "" {
			s3URL = am.AccessUrl.Url
			break
		}
	}

	if s3URL == "" {
		http.Error(w, "No S3 location found for this file", http.StatusNotFound)
		return
	}

	opts := urlmanager.SignOptions{}
	if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
		if exp, err := strconv.Atoi(expStr); err == nil {
			opts.ExpiresIn = exp
		}
	}

	signedURL, err := uM.SignURL(r.Context(), "", s3URL, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.URL.Query().Get("redirect") == "true" {
		http.Redirect(w, r, signedURL, http.StatusFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceSignedURL{URL: signedURL})
}

func handleFenceUploadBlank(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceUploadBlankRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	guid := req.GUID
	if guid == "" {
		guid = uuid.New().String()
	}

	// Check if exists
	_, err := database.GetObject(r.Context(), guid)
	if err == nil {
		// Found existing. If they provided a GUID, that's fine.
	} else {
		// Not found, create blank
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
		}
		// If authz provided
		_ = req.Authz // Reserved for future use

		if err := database.CreateObject(r.Context(), obj); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Generate a signed upload URL to a default bucket (the first one)
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil || len(creds) == 0 {
		http.Error(w, "No buckets configured for upload", http.StatusInternalServerError)
		return
	}
	bucket := creds[0].Bucket
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, guid)

	signedURL, err := uM.SignUploadURL(r.Context(), "", s3URL, urlmanager.SignOptions{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceUploadBlankResponse{
		GUID: guid,
		URL:  signedURL,
	})
}

func handleFenceUploadURL(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	bucket := r.URL.Query().Get("bucket")
	fileName := r.URL.Query().Get("file_name")

	if bucket == "" {
		creds, _ := database.ListS3Credentials(r.Context())
		if len(creds) > 0 {
			bucket = creds[0].Bucket
		}
	}

	if fileName == "" {
		fileName = fileID
	}

	if bucket == "" {
		http.Error(w, "No bucket specified or configured", http.StatusBadRequest)
		return
	}

	s3URL := fmt.Sprintf("s3://%s/%s", bucket, fileName)

	opts := urlmanager.SignOptions{}
	if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
		if exp, err := strconv.Atoi(expStr); err == nil {
			opts.ExpiresIn = exp
		}
	}

	signedURL, err := uM.SignUploadURL(r.Context(), "", s3URL, opts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceSignedURL{URL: signedURL})
}

func handleFenceMultipartInit(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceMultipartInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	guid := req.GUID
	if guid == "" {
		guid = uuid.New().String()
	}

	bucket := req.Bucket
	if bucket == "" {
		creds, _ := database.ListS3Credentials(r.Context())
		if len(creds) > 0 {
			bucket = creds[0].Bucket
		}
	}

	if bucket == "" {
		http.Error(w, "No bucket configured for upload", http.StatusInternalServerError)
		return
	}

	fileName := req.FileName
	if fileName == "" {
		fileName = guid
	}

	uploadID, err := uM.InitMultipartUpload(r.Context(), bucket, fileName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create blank record if not exists
	_, err = database.GetObject(r.Context(), guid)
	if err != nil {
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
			Name:        fileName,
		}
		database.CreateObject(r.Context(), obj)
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceMultipartInitResponse{
		GUID:     guid,
		UploadID: uploadID,
	})
}

func handleFenceMultipartUpload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceMultipartUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// We need to know which bucket/key this upload ID belongs to.
	// Fence usually gets file_id in some way but this POST has no file_id in path.
	// Indexd records might store it, but for now let's assume one default bucket
	// or we need a way to look up the uploadId.
	// Actually, Fence doesn't pass guid here. This is a bit problematic for a stateless server.
	// However, we can use the GUID from the query? No.
	// I'll assume standard bucket/key for now as a POC if not provided.
	// Wait, Fence Swagger says it takes uploadId and partNumber.
	// I'll use a hardcoded bucket if not found? No.
	// Better: I'll require a query param or something?
	// Actually, I'll look for first bucket.
	creds, _ := database.ListS3Credentials(r.Context())
	if len(creds) == 0 {
		http.Error(w, "No bucket configured", http.StatusInternalServerError)
		return
	}
	bucket := creds[0].Bucket

	// How to get the key? Usually it's the GUID.
	// I'll try to find an object that might be associated?
	// This is where a real Fence stores state.
	// For now, I'll use a placeholder and hope the user provides guid in query.
	key := r.URL.Query().Get("key")
	if key == "" {
		// Fallback: try to guess? No.
		http.Error(w, "Query parameter 'key' (GUID) required for multipart upload part signing in this implementation", http.StatusBadRequest)
		return
	}

	signedURL, err := uM.SignMultipartPart(r.Context(), bucket, key, req.UploadID, req.PartNumber)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceMultipartUploadResponse{PresignedURL: signedURL})
}

func handleFenceMultipartComplete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceMultipartCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Query parameter 'key' (GUID) required", http.StatusBadRequest)
		return
	}

	creds, _ := database.ListS3Credentials(r.Context())
	if len(creds) == 0 {
		http.Error(w, "No bucket configured", http.StatusInternalServerError)
		return
	}
	bucket := creds[0].Bucket

	var parts []urlmanager.MultipartPart
	for _, p := range req.Parts {
		parts = append(parts, urlmanager.MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	err := uM.CompleteMultipartUpload(r.Context(), bucket, key, req.UploadID, parts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func handleFenceBuckets(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var infos []fenceBucketInfo
	for _, c := range creds {
		infos = append(infos, fenceBucketInfo{
			Name:   c.Bucket,
			Region: c.Region,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(infos)
}
