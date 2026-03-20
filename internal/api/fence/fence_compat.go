package fence

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/drs-server/apigen/bucketapi"
	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/internalapi"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
)

type multipartSession struct {
	Bucket string
	Key    string
}

var multipartUploadSessions sync.Map // uploadID -> multipartSession

const bucketAdminResource = "/services/fence/buckets"

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := core.GetRequestID(r.Context())
	if err != nil {
		slog.Error("fence request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("fence request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	writeHTTPError(w, r, code, "Unauthorized", nil)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		writeAuthError(w, r)
	case errors.Is(err, core.ErrConflict):
		writeHTTPError(w, r, http.StatusConflict, err.Error(), err)
	case errors.Is(err, core.ErrNotFound):
		writeHTTPError(w, r, http.StatusNotFound, "File not found", err)
	default:
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
	}
}

func normalizeScopePath(rawPath, bucket string) (string, error) {
	p := strings.TrimSpace(rawPath)
	if p == "" {
		return "", nil
	}
	if !strings.HasPrefix(strings.ToLower(p), "s3://") {
		return "", fmt.Errorf("path must use s3://<bucket>/<prefix> format")
	}
	u, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("invalid s3 path: %w", err)
	}
	if !strings.EqualFold(u.Scheme, "s3") || strings.TrimSpace(u.Host) == "" {
		return "", fmt.Errorf("invalid s3 path")
	}
	if !strings.EqualFold(strings.TrimSpace(u.Host), strings.TrimSpace(bucket)) {
		return "", fmt.Errorf("s3 path bucket does not match bucket")
	}
	return strings.Trim(strings.TrimSpace(u.Path), "/"), nil
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
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "FenceBuckets")).Methods(http.MethodGet, http.MethodPut)

	router.Handle(base+"/buckets/{bucket}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			handleFenceDeleteBucket(w, r, database)
			return
		}
		writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
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
		writeHTTPError(w, r, http.StatusNotFound, "No S3 location found for this file", nil)
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
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	if recErr := database.RecordFileDownload(r.Context(), fileID); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(r.Context()), "file_id", fileID, "err", recErr)
	}

	if r.URL.Query().Get("redirect") == "true" {
		http.Redirect(w, r, signedURL, http.StatusFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(internalapi.FenceSignedURL{Url: &signedURL}); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleFenceUploadBlank(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.FenceUploadBlankRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}

	guid := req.GetGuid()
	if guid == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "guid is required", nil)
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

		if err := database.CreateObject(r.Context(), &core.InternalObject{
			DrsObject:      *obj,
			Authorizations: append([]string(nil), req.Authz...),
		}); err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
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
		writeHTTPError(w, r, http.StatusInternalServerError, "No buckets configured for upload", nil)
		return
	}
	bucket := creds[0].Bucket
	s3URL := fmt.Sprintf("s3://%s/%s", bucket, guid)

	signedURL, err := uM.SignUploadURL(r.Context(), "", s3URL, urlmanager.SignOptions{})
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(internalapi.FenceUploadBlankResponse{Guid: &guid, Url: &signedURL}); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
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
		writeHTTPError(w, r, http.StatusBadRequest, "No bucket specified or configured", nil)
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
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(internalapi.FenceSignedURL{Url: &signedURL}); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleFenceMultipartInit(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.FenceMultipartInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}

	guid := req.GetGuid()
	if guid == "" {
		if req.GetFileName() == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "guid or file_name is required", nil)
			return
		}
		guid = req.GetFileName()
	}

	bucket, err := resolveBucket(r, database, req.GetBucket())
	if bucket == "" {
		writeHTTPError(w, r, http.StatusInternalServerError, "No bucket configured for upload", nil)
		return
	}

	fileName := req.GetFileName()
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
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
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
		if err := database.CreateObject(r.Context(), &core.InternalObject{
			DrsObject:      *obj,
			Authorizations: []string{},
		}); err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(internalapi.FenceMultipartInitResponse{Guid: &guid, UploadId: &uploadID}); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleFenceMultipartUpload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.FenceMultipartUploadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	q := r.URL.Query()
	if req.Key == "" {
		req.Key = q.Get("key")
	}
	if req.UploadId == "" {
		req.UploadId = q.Get("uploadId")
		if req.UploadId == "" {
			req.UploadId = q.Get("upload_id")
		}
	}
	if req.PartNumber <= 0 {
		if raw := q.Get("partNumber"); raw != "" {
			if v, err := strconv.Atoi(raw); err == nil {
				req.PartNumber = int32(v)
			}
		}
	}
	if req.GetBucket() == "" {
		if b := q.Get("bucket"); b != "" {
			req.Bucket = &b
		}
	}
	if req.UploadId != "" && (req.Key == "" || req.GetBucket() == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadId); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.GetBucket() == "" {
					b := session.Bucket
					req.Bucket = &b
				}
			}
		}
	}

	if req.Key == "" || req.UploadId == "" || req.PartNumber <= 0 {
		writeHTTPError(w, r, http.StatusBadRequest, "key, uploadId, and positive partNumber are required", nil)
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

	bucket, err := resolveBucket(r, database, req.GetBucket())
	if err != nil || bucket == "" {
		writeHTTPError(w, r, http.StatusInternalServerError, "No bucket configured", nil)
		return
	}

	signedURL, err := uM.SignMultipartPart(r.Context(), bucket, req.Key, req.UploadId, req.PartNumber)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(internalapi.FenceMultipartUploadResponse{PresignedUrl: &signedURL}); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleFenceMultipartComplete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.FenceMultipartCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	q := r.URL.Query()
	if req.Key == "" {
		req.Key = q.Get("key")
	}
	if req.UploadId == "" {
		req.UploadId = q.Get("uploadId")
		if req.UploadId == "" {
			req.UploadId = q.Get("upload_id")
		}
	}
	if req.GetBucket() == "" {
		if b := q.Get("bucket"); b != "" {
			req.Bucket = &b
		}
	}
	if req.UploadId != "" && (req.Key == "" || req.GetBucket() == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadId); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.GetBucket() == "" {
					b := session.Bucket
					req.Bucket = &b
				}
			}
		}
	}

	if req.Key == "" || req.UploadId == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "key and uploadId are required", nil)
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

	bucket, err := resolveBucket(r, database, req.GetBucket())
	if err != nil || bucket == "" {
		writeHTTPError(w, r, http.StatusInternalServerError, "No bucket configured", nil)
		return
	}

	var parts []urlmanager.MultipartPart
	for _, p := range req.Parts {
		parts = append(parts, urlmanager.MultipartPart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	err = uM.CompleteMultipartUpload(r.Context(), bucket, req.Key, req.UploadId, parts)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	multipartUploadSessions.Delete(req.UploadId)
	if recErr := database.RecordFileUpload(r.Context(), req.Key); recErr != nil {
		slog.Debug("failed to record file upload metric", "request_id", core.GetRequestID(r.Context()), "key", req.Key, "err", recErr)
	}

	w.WriteHeader(http.StatusOK)
}

func handleFenceBuckets(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	if !hasAnyMethodAccess(r, []string{bucketAdminResource}, "read") {
		writeAuthError(w, r)
		return
	}
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	resp := bucketapi.BucketsResponse{
		S3BUCKETS: make(map[string]bucketapi.BucketMetadata, len(creds)),
	}
	scopes, _ := database.ListBucketScopes(r.Context())
	programsByBucket := map[string][]string{}
	for _, s := range scopes {
		res := core.ResourcePathForScope(s.Organization, s.ProjectID)
		if res == "" {
			continue
		}
		programsByBucket[s.Bucket] = append(programsByBucket[s.Bucket], res)
	}
	for _, c := range creds {
		bm := bucketapi.BucketMetadata{}
		bm.SetEndpointUrl(c.Endpoint)
		bm.SetRegion(c.Region)
		bm.SetPrograms(programsByBucket[c.Bucket])
		resp.S3BUCKETS[c.Bucket] = bm
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("fence encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleFencePutBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	if !hasAnyMethodAccess(r, []string{bucketAdminResource}, "create", "update") {
		writeAuthError(w, r)
		return
	}
	var req bucketapi.PutBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	if req.Bucket == "" || req.AccessKey == "" || req.SecretKey == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket, access_key and secret_key are required", nil)
		return
	}
	if strings.TrimSpace(req.Region) == "" || strings.TrimSpace(req.Endpoint) == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "region and endpoint are required", nil)
		return
	}
	if strings.TrimSpace(req.Organization) == "" || strings.TrimSpace(req.ProjectId) == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "organization and project_id are required", nil)
		return
	}
	prefix, err := normalizeScopePath(req.GetPath(), req.Bucket)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}
	if prefix == "" {
		prefix = strings.Trim(strings.TrimSpace(req.Organization)+"/"+strings.TrimSpace(req.ProjectId), "/")
	}
	if err := database.CreateBucketScope(r.Context(), &core.BucketScope{
		Organization: strings.TrimSpace(req.Organization),
		ProjectID:    strings.TrimSpace(req.ProjectId),
		Bucket:       strings.TrimSpace(req.Bucket),
		PathPrefix:   prefix,
	}); err != nil {
		writeDBError(w, r, err)
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
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
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
		writeHTTPError(w, r, http.StatusBadRequest, "bucket is required", nil)
		return
	}
	if err := database.DeleteS3Credential(r.Context(), bucket); err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
