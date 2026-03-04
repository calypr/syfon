package fence

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
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
	Key        string `json:"key"`
	Bucket     string `json:"bucket,omitempty"`
	UploadID   string `json:"uploadId"`
	PartNumber int32  `json:"partNumber"`
}

type fenceMultipartUploadResponse struct {
	PresignedURL string `json:"presigned_url"`
}

type fenceMultipartPart struct {
	PartNumber int32  `json:"PartNumber"`
	ETag       string `json:"ETag"`
}

type fenceMultipartCompleteRequest struct {
	Key      string               `json:"key"`
	Bucket   string               `json:"bucket,omitempty"`
	UploadID string               `json:"uploadId"`
	Parts    []fenceMultipartPart `json:"parts"`
}

type fenceBucketMetadata struct {
	EndpointURL string   `json:"endpoint_url"`
	Region      string   `json:"region"`
	Programs    []string `json:"programs,omitempty"`
}

type fenceBucketsResponse struct {
	S3Buckets map[string]fenceBucketMetadata `json:"S3_BUCKETS"`
}

type fencePutBucketRequest struct {
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Endpoint  string `json:"endpoint"`
}

type multipartSession struct {
	Bucket string
	Key    string
}

var multipartUploadSessions sync.Map // uploadID -> multipartSession

const bucketAdminResource = "/services/fence/buckets"

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	http.Error(w, "Unauthorized", code)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		writeAuthError(w, r)
	case errors.Is(err, core.ErrNotFound):
		http.Error(w, "File not found", http.StatusNotFound)
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func hasAnyMethodAccess(ctx *http.Request, resources []string, methods ...string) bool {
	if !core.IsGen3Mode(ctx.Context()) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	for _, m := range methods {
		if core.HasMethodAccess(ctx.Context(), m, resources) {
			return true
		}
	}
	return false
}

func RegisterFenceRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	for _, p := range []string{"/data", "/user/data"} {
		registerFenceDataRoutes(router, p, database, uM)
	}
	// Legacy multipart paths retained for backwards compatibility.
	registerFenceMultipartRoutes(router, "", database, uM)
}

func registerFenceDataRoutes(router *mux.Router, base string, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	router.Handle(base+"/download/{file_id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceDownload(w, r, database, uM)
	}), "FenceDownload")).Methods(http.MethodGet)

	router.Handle(base+"/upload", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceUploadBlank(w, r, database, uM)
	}), "FenceUploadBlank")).Methods(http.MethodPost)

	router.Handle(base+"/upload/{file_id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceUploadURL(w, r, database, uM)
	}), "FenceUploadURL")).Methods(http.MethodGet)

	router.Handle(base+"/multipart/init", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartInit(w, r, database, uM)
	}), "FenceMultipartInit")).Methods(http.MethodPost)

	router.Handle(base+"/multipart/upload", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartUpload(w, r, database, uM)
	}), "FenceMultipartUpload")).Methods(http.MethodPost)

	router.Handle(base+"/multipart/complete", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartComplete(w, r, database, uM)
	}), "FenceMultipartComplete")).Methods(http.MethodPost)

	router.Handle(base+"/buckets", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleFenceBuckets(w, r, database)
		case http.MethodPut:
			handleFencePutBucket(w, r, database)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	}), "FenceBuckets")).Methods(http.MethodGet, http.MethodPut)

	router.Handle(base+"/buckets/{bucket}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			handleFenceDeleteBucket(w, r, database)
			return
		}
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}), "FenceBucketDetail")).Methods(http.MethodDelete)
}

func registerFenceMultipartRoutes(router *mux.Router, base string, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	router.Handle(base+"/multipart/init", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartInit(w, r, database, uM)
	}), "FenceMultipartInitLegacy")).Methods(http.MethodPost)

	router.Handle(base+"/multipart/upload", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartUpload(w, r, database, uM)
	}), "FenceMultipartUploadLegacy")).Methods(http.MethodPost)

	router.Handle(base+"/multipart/complete", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleFenceMultipartComplete(w, r, database, uM)
	}), "FenceMultipartCompleteLegacy")).Methods(http.MethodPost)
}

func resolveBucket(ctx *http.Request, database core.DatabaseInterface, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	creds, err := database.ListS3Credentials(ctx.Context())
	if err != nil || len(creds) == 0 {
		return "", fmt.Errorf("no bucket configured")
	}
	return creds[0].Bucket, nil
}

func handleFenceDownload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	obj, err := database.GetObject(r.Context(), fileID)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if !hasAnyMethodAccess(r, obj.Authorizations, "read") {
		writeAuthError(w, r)
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
		http.Error(w, "guid is required", http.StatusBadRequest)
		return
	}

	// Check if exists
	targetResources := req.Authz
	existing, err := database.GetObject(r.Context(), guid)
	if err == nil {
		if len(existing.Authorizations) > 0 {
			targetResources = existing.Authorizations
		}
	} else {
		if !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
		// Not found, create blank
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
			writeAuthError(w, r)
			return
		}
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
		}
		// If authz provided
		_ = req.Authz // Reserved for future use

		if err := database.CreateObject(r.Context(), obj, req.Authz); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if err == nil && !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		writeAuthError(w, r)
		return
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(fenceUploadBlankResponse{
		GUID: guid,
		URL:  signedURL,
	})
}

func handleFenceUploadURL(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]
	targetResources := []string{"/data_file"}
	if fileID != "" {
		if obj, err := database.GetObject(r.Context(), fileID); err == nil {
			if len(obj.Authorizations) > 0 {
				targetResources = obj.Authorizations
			}
		} else if errors.Is(err, core.ErrUnauthorized) {
			writeDBError(w, r, err)
			return
		}
	}
	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		writeAuthError(w, r)
		return
	}

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
		if req.FileName == "" {
			http.Error(w, "guid or file_name is required", http.StatusBadRequest)
			return
		}
		guid = req.FileName
	}

	bucket, err := resolveBucket(r, database, req.Bucket)
	if bucket == "" {
		http.Error(w, "No bucket configured for upload", http.StatusInternalServerError)
		return
	}

	fileName := req.FileName
	if fileName == "" {
		fileName = guid
	}

	targetResources := []string{"/data_file"}
	if guid != "" {
		if obj, err := database.GetObject(r.Context(), guid); err == nil && len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		} else if err != nil && !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
	}
	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		writeAuthError(w, r)
		return
	}

	uploadID, err := uM.InitMultipartUpload(r.Context(), bucket, fileName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	multipartUploadSessions.Store(uploadID, multipartSession{Bucket: bucket, Key: fileName})

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
		if err := database.CreateObject(r.Context(), obj, []string{}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(fenceMultipartInitResponse{
		GUID:     guid,
		UploadID: uploadID,
	})
}

func handleFenceMultipartUpload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceMultipartUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	if req.Key == "" {
		req.Key = q.Get("key")
	}
	if req.UploadID == "" {
		req.UploadID = q.Get("uploadId")
		if req.UploadID == "" {
			req.UploadID = q.Get("upload_id")
		}
	}
	if req.PartNumber <= 0 {
		if raw := q.Get("partNumber"); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil {
				req.PartNumber = int32(v)
			}
		}
	}
	if req.Bucket == "" {
		req.Bucket = q.Get("bucket")
	}
	if req.UploadID != "" && (req.Key == "" || req.Bucket == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadID); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.Bucket == "" {
					req.Bucket = session.Bucket
				}
			}
		}
	}

	if req.Key == "" || req.UploadID == "" || req.PartNumber <= 0 {
		http.Error(w, "key, uploadId, and positive partNumber are required", http.StatusBadRequest)
		return
	}
	targetResources := []string{"/data_file"}
	if req.Key != "" {
		if obj, err := database.GetObject(r.Context(), req.Key); err == nil && len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		} else if err != nil && !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
	}
	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		writeAuthError(w, r)
		return
	}

	bucket, err := resolveBucket(r, database, req.Bucket)
	if err != nil || bucket == "" {
		http.Error(w, "No bucket configured", http.StatusInternalServerError)
		return
	}

	signedURL, err := uM.SignMultipartPart(r.Context(), bucket, req.Key, req.UploadID, req.PartNumber)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fenceMultipartUploadResponse{PresignedURL: signedURL})
}

func handleFenceMultipartComplete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req fenceMultipartCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	q := r.URL.Query()
	if req.Key == "" {
		req.Key = q.Get("key")
	}
	if req.UploadID == "" {
		req.UploadID = q.Get("uploadId")
		if req.UploadID == "" {
			req.UploadID = q.Get("upload_id")
		}
	}
	if req.Bucket == "" {
		req.Bucket = q.Get("bucket")
	}
	if req.UploadID != "" && (req.Key == "" || req.Bucket == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadID); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.Bucket == "" {
					req.Bucket = session.Bucket
				}
			}
		}
	}

	if req.Key == "" || req.UploadID == "" {
		http.Error(w, "key and uploadId are required", http.StatusBadRequest)
		return
	}
	targetResources := []string{"/data_file"}
	if req.Key != "" {
		if obj, err := database.GetObject(r.Context(), req.Key); err == nil && len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		} else if err != nil && !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
	}
	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		writeAuthError(w, r)
		return
	}

	bucket, err := resolveBucket(r, database, req.Bucket)
	if err != nil || bucket == "" {
		http.Error(w, "No bucket configured", http.StatusInternalServerError)
		return
	}

	var parts []urlmanager.MultipartPart
	for _, p := range req.Parts {
		parts = append(parts, urlmanager.MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	err = uM.CompleteMultipartUpload(r.Context(), bucket, req.Key, req.UploadID, parts)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	multipartUploadSessions.Delete(req.UploadID)

	w.WriteHeader(http.StatusOK)
}

func handleFenceBuckets(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	if !hasAnyMethodAccess(r, []string{bucketAdminResource}, "read") {
		writeAuthError(w, r)
		return
	}
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := fenceBucketsResponse{
		S3Buckets: make(map[string]fenceBucketMetadata, len(creds)),
	}
	for _, c := range creds {
		resp.S3Buckets[c.Bucket] = fenceBucketMetadata{
			Region: c.Region,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleFencePutBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	if !hasAnyMethodAccess(r, []string{bucketAdminResource}, "create", "update") {
		writeAuthError(w, r)
		return
	}
	var req fencePutBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if req.Bucket == "" || req.AccessKey == "" || req.SecretKey == "" {
		http.Error(w, "bucket, access_key and secret_key are required", http.StatusBadRequest)
		return
	}
	cred := &core.S3Credential{
		Bucket:    req.Bucket,
		Region:    req.Region,
		AccessKey: req.AccessKey,
		SecretKey: req.SecretKey,
		Endpoint:  req.Endpoint,
	}
	if err := database.SaveS3Credential(r.Context(), cred); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func handleFenceDeleteBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	if !hasAnyMethodAccess(r, []string{bucketAdminResource}, "delete") {
		writeAuthError(w, r)
		return
	}
	bucket := mux.Vars(r)["bucket"]
	if bucket == "" {
		http.Error(w, "bucket is required", http.StatusBadRequest)
		return
	}
	if err := database.DeleteS3Credential(r.Context(), bucket); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
