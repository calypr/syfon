package internaldrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/google/uuid"
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

func (s *InternalServer) InternalUploadBlank(ctx context.Context, request internalapi.InternalUploadBlankRequestObject) (internalapi.InternalUploadBlankResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalUploadBlank400Response{}, nil
	}

	guid := ""
	if req.Guid != nil {
		guid = strings.TrimSpace(*req.Guid)
	}
	if guid == "" {
		return internalapi.InternalUploadBlank400Response{}, nil
	}

	targetResources := make([]string, 0)
	if req.Authz != nil {
		targetResources = *req.Authz
	}
	existing, err := resolveObjectByIDOrChecksum(s.database, ctx, guid)
	if err == nil {
		guid = strings.TrimSpace(existing.Id)
		if len(existing.Authorizations) > 0 {
			targetResources = existing.Authorizations
		}
	} else {
		if !errors.Is(err, core.ErrNotFound) {
			return nil, err
		}
		// Not found, create blank
		if len(targetResources) == 0 {
			targetResources = []string{"/data_file"}
		}
		if !core.HasMethodAccess(ctx, "create", targetResources) && !core.HasMethodAccess(ctx, "file_upload", targetResources) {
			return internalapi.InternalUploadBlank403Response{}, nil
		}
		if _, parseErr := uuid.Parse(guid); parseErr != nil {
			if looksLikeSHA256(guid) {
				guid = core.MintObjectIDFromChecksum(guid, targetResources)
			} else {
				guid = uuid.NewString()
			}
		}
		now := time.Now()
		obj := &drs.DrsObject{
			Id:          guid,
			CreatedTime: now,
			UpdatedTime: &now,
			Version:     core.Ptr("1"),
		}

		if err := s.database.CreateObject(ctx, &core.InternalObject{
			DrsObject:      *obj,
			Authorizations: targetResources,
		}); err != nil {
			return nil, err
		}
	}
	if err == nil && !core.HasMethodAccess(ctx, "update", targetResources) && !core.HasMethodAccess(ctx, "file_upload", targetResources) {
		return internalapi.InternalUploadBlank403Response{}, nil
	}

	creds, err := s.database.ListS3Credentials(ctx)
	if err != nil || len(creds) == 0 {
		return nil, fmt.Errorf("No buckets configured for upload")
	}
	cred := creds[0]
	objectURL, err := objectURLForCredential(&cred, guid)
	if err != nil {
		return internalapi.InternalUploadBlank400Response{}, nil
	}

	if s.uM == nil {
		return nil, fmt.Errorf("URL Manager not initialized")
	}
	signedURL, err := s.uM.SignUploadURL(ctx, cred.Bucket, objectURL, urlmanager.SignOptions{})
	if err != nil {
		return nil, err
	}

	return internalapi.InternalUploadBlank201JSONResponse{
		Bucket: &cred.Bucket,
		Guid:   &guid,
		Url:    &signedURL,
	}, nil
}

func (s *InternalServer) InternalUploadURL(ctx context.Context, request internalapi.InternalUploadURLRequestObject) (internalapi.InternalUploadURLResponseObject, error) {
	fileID := request.FileId
	params := request.Params
	targetResources := []string{"/data_file"}
	if fileID != "" {
		if obj, err := s.database.GetObject(ctx, fileID); err == nil {
			if len(obj.Authorizations) > 0 {
				targetResources = obj.Authorizations
			}
		} else if errors.Is(err, core.ErrUnauthorized) {
			if authStatusCodeForContext(ctx) == http.StatusUnauthorized {
				return internalapi.InternalUploadURL401Response{}, nil
			}
			return internalapi.InternalUploadURL403Response{}, nil
		}
	}
	if !core.HasMethodAccess(ctx, "update", targetResources) && !core.HasMethodAccess(ctx, "file_upload", targetResources) {
		if authStatusCodeForContext(ctx) == http.StatusUnauthorized {
			return internalapi.InternalUploadURL401Response{}, nil
		}
		return internalapi.InternalUploadURL403Response{}, nil
	}

	bucket := ""
	if params.Bucket != nil {
		bucket = *params.Bucket
	}
	fileName := ""
	if params.FileName != nil {
		fileName = *params.FileName
	}

	if bucket == "" {
		creds, _ := s.database.ListS3Credentials(ctx)
		if len(creds) > 0 {
			bucket = creds[0].Bucket
		}
	}

	if fileName == "" {
		if resolvedKey, ok := resolveObjectRemotePathWithCtx(s.database, ctx, fileID, bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = fileID
		}
	}

	if bucket == "" {
		return internalapi.InternalUploadURL400Response{}, nil
	}

	cred, err := s.database.GetS3Credential(ctx, bucket)
	if err != nil {
		return internalapi.InternalUploadURL400Response{}, nil
	}
	objectURL, err := objectURLForCredential(cred, fileName)
	if err != nil {
		return internalapi.InternalUploadURL400Response{}, nil
	}

	opts := urlmanager.SignOptions{}
	if params.ExpiresIn != nil {
		opts.ExpiresIn = time.Duration(*params.ExpiresIn) * time.Second
	}

	if s.uM == nil {
		return nil, fmt.Errorf("URL Manager not initialized")
	}
	signedURL, err := s.uM.SignUploadURL(ctx, cred.Bucket, objectURL, opts)
	if err != nil {
		return nil, err
	}

	return internalapi.InternalUploadURL200JSONResponse{Url: &signedURL}, nil
}

func (s *InternalServer) InternalUploadBulk(ctx context.Context, request internalapi.InternalUploadBulkRequestObject) (internalapi.InternalUploadBulkResponseObject, error) {
	req := request.Body
	if req == nil {
		return internalapi.InternalUploadBulk400Response{}, nil
	}
	if len(req.Requests) == 0 {
		return internalapi.InternalUploadBulk400Response{}, nil
	}

	results := make([]internalapi.InternalUploadBulkResult, 0, len(req.Requests))
	credCache := uploadBulkCredentialCache{
		credentials: make(map[string]core.S3Credential),
	}
	if creds, err := s.database.ListS3Credentials(ctx); err == nil {
		for i, cred := range creds {
			b := strings.TrimSpace(cred.Bucket)
			if b == "" {
				continue
			}
			if i == 0 {
				credCache.defaultBucket = b
			}
			credCache.credentials[b] = cred
		}
	}

	for _, item := range req.Requests {
		result := s.signInternalUploadBulkItem(ctx, item, &credCache)
		results = append(results, result)
	}

	for _, result := range results {
		if result.Status != int32(http.StatusOK) {
			return internalapi.InternalUploadBulk207JSONResponse{Results: &results}, nil
		}
	}
	return internalapi.InternalUploadBulk200JSONResponse{Results: &results}, nil
}

func (s *InternalServer) signInternalUploadBulkItem(ctx context.Context, item internalapi.InternalUploadBulkItem, credCache *uploadBulkCredentialCache) internalapi.InternalUploadBulkResult {
	result := internalapi.InternalUploadBulkResult{}
	result.FileId = strings.TrimSpace(item.FileId)
	result.Status = int32(http.StatusOK)
	if result.FileId == "" {
		result.Status = int32(http.StatusBadRequest)
		errStr := "file_id is required"
		result.Error = &errStr
		return result
	}

	targetResources := []string{"/data_file"}
	if obj, err := s.database.GetObject(ctx, result.FileId); err == nil {
		if len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		}
	} else if errors.Is(err, core.ErrUnauthorized) {
		result.Status = int32(http.StatusUnauthorized)
		errStr := "Unauthorized"
		result.Error = &errStr
		return result
	}

	if !core.HasMethodAccess(ctx, "update", targetResources) && !core.HasMethodAccess(ctx, "file_upload", targetResources) {
		result.Status = int32(authStatusCodeForContext(ctx))
		errStr := "Unauthorized"
		result.Error = &errStr
		return result
	}

	bucket := ""
	if item.Bucket != nil {
		bucket = strings.TrimSpace(*item.Bucket)
	}
	if bucket == "" {
		if credCache != nil && strings.TrimSpace(credCache.defaultBucket) != "" {
			bucket = strings.TrimSpace(credCache.defaultBucket)
		} else {
			creds, _ := s.database.ListS3Credentials(ctx)
			if len(creds) > 0 {
				bucket = strings.TrimSpace(creds[0].Bucket)
			}
		}
	}
	if bucket != "" {
		result.Bucket = &bucket
	}

	fileName := ""
	if item.FileName != nil {
		fileName = strings.TrimSpace(*item.FileName)
	}
	if fileName == "" {
		if resolvedKey, ok := resolveObjectRemotePathWithCtx(s.database, ctx, result.FileId, bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = result.FileId
		}
	}
	if fileName != "" {
		result.FileName = &fileName
	}

	if bucket == "" {
		result.Status = int32(http.StatusBadRequest)
		errStr := "No bucket specified or configured"
		result.Error = &errStr
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
		fresh, err := s.database.GetS3Credential(ctx, bucket)
		if err != nil {
			result.Status = int32(http.StatusBadRequest)
			errStr := "bucket credential not found"
			result.Error = &errStr
			return result
		}
		cred = fresh
		if credCache != nil && strings.TrimSpace(cred.Bucket) != "" {
			credCache.credentials[strings.TrimSpace(cred.Bucket)] = *cred
		}
	}
	objectURL, err := objectURLForCredential(cred, fileName)
	if err != nil {
		result.Status = int32(http.StatusBadRequest)
		errStr := err.Error()
		result.Error = &errStr
		return result
	}

	opts := urlmanager.SignOptions{}
	if item.ExpiresIn != nil {
		opts.ExpiresIn = time.Duration(*item.ExpiresIn) * time.Second
	}
	if s.uM == nil {
		result.Status = int32(http.StatusInternalServerError)
		errStr := "URL Manager not initialized"
		result.Error = &errStr
		return result
	}
	signedURL, err := s.uM.SignUploadURL(ctx, cred.Bucket, objectURL, opts)
	if err != nil {
		result.Status = int32(http.StatusInternalServerError)
		errStr := err.Error()
		result.Error = &errStr
		return result
	}

	result.Url = &signedURL
	return result
}

func signInternalUploadBulkItem(r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager, item internalapi.InternalUploadBulkItem, credCache *uploadBulkCredentialCache) internalapi.InternalUploadBulkResult {
	result := internalapi.InternalUploadBulkResult{}
	result.FileId = strings.TrimSpace(item.FileId)
	result.Status = int32(http.StatusOK)
	if result.FileId == "" {
		result.Status = int32(http.StatusBadRequest)
		errStr := "file_id is required"
		result.Error = &errStr
		return result
	}

	targetResources := []string{"/data_file"}
	if obj, err := database.GetObject(r.Context(), result.FileId); err == nil {
		if len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		}
	} else if errors.Is(err, core.ErrUnauthorized) {
		result.Status = int32(authStatusCodeForRequest(r))
		errStr := "Unauthorized"
		result.Error = &errStr
		return result
	}

	if !hasAnyMethodAccess(r, targetResources, "file_upload", "create", "update") {
		result.Status = int32(authStatusCodeForRequest(r))
		errStr := "Unauthorized"
		result.Error = &errStr
		return result
	}

	bucket := ""
	if item.Bucket != nil {
		bucket = strings.TrimSpace(*item.Bucket)
	}
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
		result.Bucket = &bucket
	}

	fileName := ""
	if item.FileName != nil {
		fileName = strings.TrimSpace(*item.FileName)
	}
	if fileName == "" {
		if resolvedKey, ok := resolveObjectRemotePath(database, r, result.FileId, bucket); ok {
			fileName = resolvedKey
		} else {
			fileName = result.FileId
		}
	}
	if fileName != "" {
		result.FileName = &fileName
	}

	if bucket == "" {
		result.Status = int32(http.StatusBadRequest)
		errStr := "No bucket specified or configured"
		result.Error = &errStr
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
			result.Status = int32(http.StatusBadRequest)
			errStr := "bucket credential not found"
			result.Error = &errStr
			return result
		}
		cred = fresh
		if credCache != nil && strings.TrimSpace(cred.Bucket) != "" {
			credCache.credentials[strings.TrimSpace(cred.Bucket)] = *cred
		}
	}
	objectURL, err := objectURLForCredential(cred, fileName)
	if err != nil {
		result.Status = int32(http.StatusBadRequest)
		errStr := err.Error()
		result.Error = &errStr
		return result
	}

	opts := urlmanager.SignOptions{}
	if item.ExpiresIn != nil {
		opts.ExpiresIn = time.Duration(*item.ExpiresIn) * time.Second
	}
	signedURL, err := uM.SignUploadURL(r.Context(), cred.Bucket, objectURL, opts)
	if err != nil {
		result.Status = int32(http.StatusInternalServerError)
		errStr := err.Error()
		result.Error = &errStr
		return result
	}

	result.Url = &signedURL
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

	requestGUID := ""
	if req.Guid != nil {
		requestGUID = strings.TrimSpace(*req.Guid)
	}
	fileName := ""
	if req.FileName != nil {
		fileName = strings.TrimSpace(*req.FileName)
	}
	if requestGUID == "" && fileName == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "guid or file_name is required", nil)
		return
	}

	bucket, err := resolveBucket(r, database, core.StringVal(req.Bucket))
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
		} else if resolvedKey, ok := resolveObjectRemotePath(database, r, guid, bucket); ok {
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
			UpdatedTime: &now,
			Version:     core.Ptr("1"),
			Name:        core.Ptr(fileName),
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
	if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
		if b := q.Get("bucket"); b != "" {
			req.Bucket = &b
		}
	}
	if req.UploadId != "" && (req.Key == "" || req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadId); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
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

	bucket, err := resolveBucket(r, database, core.StringVal(req.Bucket))
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
	if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
		if b := q.Get("bucket"); b != "" {
			req.Bucket = &b
		}
	}
	if req.UploadId != "" && (req.Key == "" || req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "") {
		if raw, ok := multipartUploadSessions.Load(req.UploadId); ok {
			if session, ok := raw.(multipartSession); ok {
				if req.Key == "" {
					req.Key = session.Key
				}
				if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
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

	bucket, err := resolveBucket(r, database, core.StringVal(req.Bucket))
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

func handleInternalUploadBlank(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalUploadBlankRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	server := NewInternalServer(database, uM)
	resp, err := server.InternalUploadBlank(r.Context(), internalapi.InternalUploadBlankRequestObject{Body: &req})
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	switch out := resp.(type) {
	case internalapi.InternalUploadBlank201JSONResponse:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	case internalapi.InternalUploadBlank400Response:
		w.WriteHeader(http.StatusBadRequest)
	case internalapi.InternalUploadBlank401Response:
		w.WriteHeader(http.StatusUnauthorized)
	case internalapi.InternalUploadBlank403Response:
		w.WriteHeader(http.StatusForbidden)
	case internalapi.InternalUploadBlank500Response:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func handleInternalUploadURL(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	params := internalapi.InternalUploadURLParams{}
	if bucket := strings.TrimSpace(r.URL.Query().Get("bucket")); bucket != "" {
		params.Bucket = &bucket
	}
	if fileName := strings.TrimSpace(r.URL.Query().Get("filename")); fileName != "" {
		params.FileName = &fileName
	}
	if raw := strings.TrimSpace(r.URL.Query().Get("expires_in")); raw != "" {
		if expiresIn, err := strconv.Atoi(raw); err == nil {
			v := int32(expiresIn)
			params.ExpiresIn = &v
		}
	}
	server := NewInternalServer(database, uM)
	resp, err := server.InternalUploadURL(r.Context(), internalapi.InternalUploadURLRequestObject{
		FileId: routeutil.PathParam(r, "file_id"),
		Params: params,
	})
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	switch out := resp.(type) {
	case internalapi.InternalUploadURL200JSONResponse:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(out)
	case internalapi.InternalUploadURL400Response:
		w.WriteHeader(http.StatusBadRequest)
	case internalapi.InternalUploadURL401Response:
		w.WriteHeader(http.StatusUnauthorized)
	case internalapi.InternalUploadURL403Response:
		w.WriteHeader(http.StatusForbidden)
	case internalapi.InternalUploadURL404Response:
		w.WriteHeader(http.StatusNotFound)
	case internalapi.InternalUploadURL500Response:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func handleInternalUploadBulk(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	var req internalapi.InternalUploadBulkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	server := NewInternalServer(database, uM)
	resp, err := server.InternalUploadBulk(r.Context(), internalapi.InternalUploadBulkRequestObject{Body: &req})
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	switch out := resp.(type) {
	case internalapi.InternalUploadBulk200JSONResponse:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(out)
	case internalapi.InternalUploadBulk207JSONResponse:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMultiStatus)
		_ = json.NewEncoder(w).Encode(out)
	case internalapi.InternalUploadBulk400Response:
		w.WriteHeader(http.StatusBadRequest)
	case internalapi.InternalUploadBulk401Response:
		w.WriteHeader(http.StatusUnauthorized)
	case internalapi.InternalUploadBulk403Response:
		w.WriteHeader(http.StatusForbidden)
	case internalapi.InternalUploadBulk500Response:
		w.WriteHeader(http.StatusInternalServerError)
	}
}
