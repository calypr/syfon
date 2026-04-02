package internaldrs

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type multipartSession struct {
	Bucket string
	Key    string
}

var multipartUploadSessions sync.Map // uploadID -> multipartSession

type uploadBulkCredentialCache struct {
	defaultBucket string
	credentials   map[string]core.S3Credential
}

func handleInternalUploadBlank(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalUploadBlankRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}

	guid := strings.TrimSpace(req.GetGuid())
	if guid == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "guid is required", nil)
		return
	}

	targetResources := req.Authz
	existing, err := resolveObjectByIDOrChecksum(database, r.Context(), guid)
	if err == nil {
		guid = strings.TrimSpace(existing.Id)
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
		if _, parseErr := uuid.Parse(guid); parseErr != nil {
			if looksLikeSHA256(guid) {
				guid = core.MintObjectIDFromChecksum(guid, req.Authz)
			} else {
				guid = uuid.NewString()
			}
		}
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
		}

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
	cred := creds[0]
	objectURL, err := objectURLForCredential(&cred, guid)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}

	signedURL, err := uM.SignUploadURL(r.Context(), cred.Bucket, objectURL, urlmanager.SignOptions{})
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	resp := internalapi.InternalUploadBlankResponse{}
	resp.SetGuid(guid)
	resp.SetUrl(signedURL)
	resp.SetBucket(cred.Bucket)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalUploadURL(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
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
		if resolvedKey, ok := resolveObjectS3Key(database, r, fileID, bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = fileID
		}
	}

	if bucket == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "No bucket specified or configured", nil)
		return
	}

	cred, err := database.GetS3Credential(r.Context(), bucket)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket credential not found", err)
		return
	}
	objectURL, err := objectURLForCredential(cred, fileName)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
		return
	}

	opts := urlmanager.SignOptions{}
	if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
		if exp, err := strconv.Atoi(expStr); err == nil {
			opts.ExpiresIn = exp
		}
	}

	signedURL, err := uM.SignUploadURL(r.Context(), cred.Bucket, objectURL, opts)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(internalapi.InternalSignedURL{Url: &signedURL}); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalUploadBulk(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalUploadBulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	if len(req.GetRequests()) == 0 {
		writeHTTPError(w, r, http.StatusBadRequest, "requests cannot be empty", nil)
		return
	}

	resp := internalapi.InternalUploadBulkResponse{}
	resp.SetResults(make([]internalapi.InternalUploadBulkResult, 0, len(req.GetRequests())))
	aggregateStatus := http.StatusOK
	credCache := uploadBulkCredentialCache{
		credentials: make(map[string]core.S3Credential),
	}
	if creds, err := database.ListS3Credentials(r.Context()); err == nil {
		for i, cred := range creds {
			bucket := strings.TrimSpace(cred.Bucket)
			if bucket == "" {
				continue
			}
			if i == 0 {
				credCache.defaultBucket = bucket
			}
			credCache.credentials[bucket] = cred
		}
	}

	for _, item := range req.GetRequests() {
		result := signInternalUploadBulkItem(r, database, uM, item, &credCache)
		if result.GetStatus() != int32(http.StatusOK) {
			aggregateStatus = http.StatusMultiStatus
		}
		resp.Results = append(resp.Results, result)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(aggregateStatus)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func signInternalUploadBulkItem(r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager, item internalapi.InternalUploadBulkItem, credCache *uploadBulkCredentialCache) internalapi.InternalUploadBulkResult {
	result := internalapi.InternalUploadBulkResult{}
	result.SetFileId(strings.TrimSpace(item.GetFileId()))
	result.SetStatus(int32(http.StatusOK))
	if result.GetFileId() == "" {
		result.SetStatus(int32(http.StatusBadRequest))
		result.SetError("file_id is required")
		return result
	}

	targetResources := []string{"/data_file"}
	if obj, err := database.GetObject(r.Context(), result.GetFileId()); err == nil {
		if len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		}
	} else if errors.Is(err, core.ErrUnauthorized) {
		result.SetStatus(int32(authStatusCodeForRequest(r)))
		result.SetError("Unauthorized")
		return result
	}

	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		result.SetStatus(int32(authStatusCodeForRequest(r)))
		result.SetError("Unauthorized")
		return result
	}

	bucket := strings.TrimSpace(item.GetBucket())
	if bucket == "" {
		if credCache != nil && strings.TrimSpace(credCache.defaultBucket) != "" {
			bucket = strings.TrimSpace(credCache.defaultBucket)
		} else {
			creds, _ := database.ListS3Credentials(r.Context())
			if len(creds) > 0 {
				bucket = strings.TrimSpace(creds[0].Bucket)
			}
		}
	}
	if bucket != "" {
		result.SetBucket(bucket)
	}

	fileName := strings.TrimSpace(item.GetFileName())
	if fileName == "" {
		if resolvedKey, ok := resolveObjectS3Key(database, r, result.GetFileId(), bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = result.GetFileId()
		}
	}
	if fileName != "" {
		result.SetFileName(fileName)
	}

	if bucket == "" {
		result.SetStatus(int32(http.StatusBadRequest))
		result.SetError("No bucket specified or configured")
		return result
	}

	var cred *core.S3Credential
	if credCache != nil {
		if cached, ok := credCache.credentials[bucket]; ok {
			c := cached
			cred = &c
		}
	}
	if cred == nil {
		fresh, err := database.GetS3Credential(r.Context(), bucket)
		if err != nil {
			result.SetStatus(int32(http.StatusBadRequest))
			result.SetError("bucket credential not found")
			return result
		}
		cred = fresh
		if credCache != nil && strings.TrimSpace(cred.Bucket) != "" {
			credCache.credentials[strings.TrimSpace(cred.Bucket)] = *cred
		}
	}
	objectURL, err := objectURLForCredential(cred, fileName)
	if err != nil {
		result.SetStatus(int32(http.StatusBadRequest))
		result.SetError(err.Error())
		return result
	}

	opts := urlmanager.SignOptions{}
	if exp, ok := item.GetExpiresInOk(); ok {
		opts.ExpiresIn = int(*exp)
	}
	signedURL, err := uM.SignUploadURL(r.Context(), cred.Bucket, objectURL, opts)
	if err != nil {
		result.SetStatus(int32(http.StatusInternalServerError))
		result.SetError(err.Error())
		return result
	}

	result.SetUrl(signedURL)
	return result
}

func authStatusCodeForRequest(r *http.Request) int {
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		return http.StatusUnauthorized
	}
	return http.StatusForbidden
}

func handleInternalMultipartInit(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalMultipartInitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}

	requestGUID := strings.TrimSpace(req.GetGuid())
	fileName := strings.TrimSpace(req.GetFileName())
	if requestGUID == "" && fileName == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "guid or file_name is required", nil)
		return
	}

	bucket, err := resolveBucket(r, database, req.GetBucket())
	if bucket == "" {
		writeHTTPError(w, r, http.StatusInternalServerError, "No bucket configured for upload", nil)
		return
	}

	checksumHint := checksumHintFromInputs(requestGUID, fileName)

	var existingObj *core.InternalObject
	if requestGUID != "" {
		obj, err := resolveObjectByIDOrChecksum(database, r.Context(), requestGUID)
		if err != nil && !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
		existingObj = obj
	}
	if existingObj == nil && checksumHint != "" {
		obj, err := resolveObjectByIDOrChecksum(database, r.Context(), checksumHint)
		if err != nil && !errors.Is(err, core.ErrNotFound) {
			writeDBError(w, r, err)
			return
		}
		existingObj = obj
	}

	guid := requestGUID
	if existingObj != nil && strings.TrimSpace(existingObj.Id) != "" {
		guid = strings.TrimSpace(existingObj.Id)
	} else if guid == "" {
		guid = uuid.NewString()
	} else if _, parseErr := uuid.Parse(guid); parseErr != nil {
		if checksumHint != "" && looksLikeSHA256(checksumHint) {
			guid = core.MintObjectIDFromChecksum(checksumHint, targetResourcesFromObject(existingObj))
		} else if looksLikeSHA256(guid) {
			guid = core.MintObjectIDFromChecksum(guid, targetResourcesFromObject(existingObj))
		} else {
			guid = uuid.NewString()
		}
	}

	if fileName == "" {
		if checksumHint != "" {
			fileName = checksumHint
		} else if resolvedKey, ok := resolveObjectS3Key(database, r, guid, bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = guid
		}
	}

	targetResources := []string{"/data_file"}
	if existingObj != nil && len(existingObj.Authorizations) > 0 {
		targetResources = existingObj.Authorizations
	} else if obj, err := database.GetObject(r.Context(), guid); err == nil && len(obj.Authorizations) > 0 {
		targetResources = obj.Authorizations
	} else if err != nil && !errors.Is(err, core.ErrNotFound) {
		writeDBError(w, r, err)
		return
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
	if existingObj == nil {
		_, err = database.GetObject(r.Context(), guid)
	}
	if existingObj == nil && err != nil {
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: now,
			Version:     "1",
			Name:        fileName,
		}
		if checksumHint != "" {
			obj.Checksums = []drs.Checksum{{Type: "sha256", Checksum: checksumHint}}
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
	if err := json.NewEncoder(w).Encode(internalapi.InternalMultipartInitResponse{Guid: &guid, UploadId: &uploadID}); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalMultipartUpload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalMultipartUploadRequest
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
	if err := json.NewEncoder(w).Encode(internalapi.InternalMultipartUploadResponse{PresignedUrl: &signedURL}); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalMultipartComplete(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalMultipartCompleteRequest
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
