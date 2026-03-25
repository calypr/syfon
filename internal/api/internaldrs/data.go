package internaldrs

import (
	"context"
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
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

type multipartSession struct {
	Bucket string
	Key    string
}

var multipartUploadSessions sync.Map // uploadID -> multipartSession

const bucketAdminResource = "/services/internal/buckets"

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

func hasGlobalBucketAdminAccess(r *http.Request, methods ...string) bool {
	return hasAnyMethodAccess(r, []string{bucketAdminResource}, methods...)
}

func scopeResource(org, project string) string {
	return strings.TrimSpace(core.ResourcePathForScope(org, project))
}

func hasScopedBucketAccess(r *http.Request, scope core.BucketScope, methods ...string) bool {
	res := scopeResource(scope.Organization, scope.ProjectID)
	if res == "" {
		return false
	}
	return hasAnyMethodAccess(r, []string{res}, methods...)
}

func RegisterInternalDataRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	// Data routes exposed under /data to match gateway contract.
	router.Handle("/data/download/{file_id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDownload(w, r, database, uM)
	}), "InternalDownload")).Methods(http.MethodGet)

	router.Handle("/data/upload", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadBlank(w, r, database, uM)
	}), "InternalUploadBlank")).Methods(http.MethodPost)

	router.Handle("/data/upload/{file_id}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadURL(w, r, database, uM)
	}), "InternalUploadURL")).Methods(http.MethodGet)

	router.Handle("/data/multipart/init", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartInit(w, r, database, uM)
	}), "InternalMultipartInit")).Methods(http.MethodPost)

	router.Handle("/data/multipart/upload", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartUpload(w, r, database, uM)
	}), "InternalMultipartUpload")).Methods(http.MethodPost)

	router.Handle("/data/multipart/complete", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartComplete(w, r, database, uM)
	}), "InternalMultipartComplete")).Methods(http.MethodPost)

	// Bucket endpoints.
	router.Handle("/data/buckets", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleInternalBuckets(w, r, database)
		case http.MethodPut:
			handleInternalPutBucket(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "InternalBuckets")).Methods(http.MethodGet, http.MethodPut)

	router.Handle("/data/buckets/{bucket}", drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			handleInternalDeleteBucket(w, r, database)
			return
		}
		writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
	}), "InternalBucketDetail")).Methods(http.MethodDelete)
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

func providerForCredential(cred *core.S3Credential) string {
	if cred == nil || strings.TrimSpace(cred.Provider) == "" {
		return "s3"
	}
	p := strings.ToLower(strings.TrimSpace(cred.Provider))
	switch p {
	case "s3", "gcs", "azure", "file":
		return p
	case "gs":
		return "gcs"
	default:
		return p
	}
}

func objectURLForCredential(cred *core.S3Credential, key string) (string, error) {
	if cred == nil {
		return "", fmt.Errorf("credential is required")
	}
	cleanKey := strings.TrimPrefix(strings.TrimSpace(key), "/")
	switch providerForCredential(cred) {
	case "s3":
		return fmt.Sprintf("s3://%s/%s", cred.Bucket, cleanKey), nil
	case "gcs":
		return fmt.Sprintf("gs://%s/%s", cred.Bucket, cleanKey), nil
	case "azure":
		return fmt.Sprintf("azblob://%s/%s", cred.Bucket, cleanKey), nil
	case "file":
		root := strings.TrimSpace(cred.Endpoint)
		if root != "" {
			root = strings.TrimSuffix(root, "/")
			return fmt.Sprintf("%s/%s", root, cleanKey), nil
		}
		return fmt.Sprintf("file:///%s/%s", strings.TrimPrefix(cred.Bucket, "/"), cleanKey), nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", providerForCredential(cred))
	}
}

func resolveObjectS3Key(database core.DatabaseInterface, ctx *http.Request, objectID string, bucket string) (string, bool) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(bucket) == "" {
		return "", false
	}
	obj, err := resolveObjectByIDOrChecksum(database, ctx.Context(), objectID)
	if err != nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	for _, am := range obj.AccessMethods {
		raw := strings.TrimSpace(am.AccessUrl.Url)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || !strings.EqualFold(u.Scheme, "s3") {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(u.Host), targetBucket) {
			continue
		}
		key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
		if key != "" {
			return key, true
		}
	}
	return "", false
}

func looksLikeSHA256(v string) bool {
	s := strings.TrimSpace(strings.ToLower(v))
	if len(s) != 64 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			continue
		}
		return false
	}
	return true
}

func checksumHintFromInputs(guid, fileName string) string {
	g := strings.TrimSpace(guid)
	if looksLikeSHA256(g) {
		return g
	}
	f := strings.TrimSpace(fileName)
	if looksLikeSHA256(f) {
		return f
	}
	parts := strings.Split(strings.Trim(f, "/"), "/")
	if len(parts) > 0 {
		last := strings.TrimSpace(parts[len(parts)-1])
		if looksLikeSHA256(last) {
			return last
		}
	}
	return ""
}

func targetResourcesFromObject(obj *core.InternalObject) []string {
	if obj == nil || len(obj.Authorizations) == 0 {
		return nil
	}
	return append([]string(nil), obj.Authorizations...)
}

func resolveObjectByIDOrChecksum(database core.DatabaseInterface, ctx context.Context, objectID string) (*core.InternalObject, error) {
	// Checksum-first resolution: internal-compatible routes are commonly called with OID.
	byChecksum, err := database.GetObjectsByChecksum(ctx, objectID)
	if err != nil {
		return nil, err
	}
	if len(byChecksum) > 0 {
		objCopy := byChecksum[0]
		return &objCopy, nil
	}

	// Legacy fallback for UUID/DID based lookups.
	obj, err := database.GetObject(ctx, objectID)
	if err == nil {
		return obj, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return nil, err
	}
	return nil, core.ErrNotFound
}

func handleInternalDownload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	vars := mux.Vars(r)
	fileID := vars["file_id"]

	obj, err := resolveObjectByIDOrChecksum(database, r.Context(), fileID)
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

	bucketID := ""
	if parsed, parseErr := url.Parse(s3URL); parseErr == nil {
		bucketID = parsed.Host
	}
	signedURL, err := uM.SignURL(r.Context(), bucketID, s3URL, opts)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	if recErr := database.RecordFileDownload(r.Context(), obj.Id); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(r.Context()), "file_id", obj.Id, "err", recErr)
	}

	if r.URL.Query().Get("redirect") == "true" {
		http.Redirect(w, r, signedURL, http.StatusFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(internalapi.InternalSignedURL{Url: &signedURL}); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
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
	if err := json.NewEncoder(w).Encode(internalapi.InternalUploadBlankResponse{Guid: &guid, Url: &signedURL}); err != nil {
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

func handleInternalBuckets(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	creds, err := database.ListS3Credentials(r.Context())
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	scopes, _ := database.ListBucketScopes(r.Context())

	allowedBuckets := map[string]bool{}
	allowAll := !core.IsGen3Mode(r.Context()) || hasGlobalBucketAdminAccess(r, "read")
	if !allowAll {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		for _, s := range scopes {
			if hasScopedBucketAccess(r, s, "read", "create", "update", "delete", "file_upload") {
				allowedBuckets[s.Bucket] = true
			}
		}
		if len(allowedBuckets) == 0 {
			writeAuthError(w, r)
			return
		}
	}

	resp := map[string]any{
		"S3_BUCKETS": map[string]map[string]any{},
	}
	outBuckets := resp["S3_BUCKETS"].(map[string]map[string]any)
	programsByBucket := map[string][]string{}
	for _, s := range scopes {
		if !allowAll && !allowedBuckets[s.Bucket] {
			continue
		}
		res := core.ResourcePathForScope(s.Organization, s.ProjectID)
		if res == "" {
			continue
		}
		programsByBucket[s.Bucket] = append(programsByBucket[s.Bucket], res)
	}
	for _, c := range creds {
		if !allowAll && !allowedBuckets[c.Bucket] {
			continue
		}
		outBuckets[c.Bucket] = map[string]any{
			"endpoint_url": c.Endpoint,
			"provider":     providerForCredential(&c),
			"region":       c.Region,
			"programs":     programsByBucket[c.Bucket],
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		slog.Error("internal encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
	}
}

func handleInternalPutBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	var req bucketapi.PutBucketRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid request", nil)
		return
	}
	provider := "s3"
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err == nil {
		if v, ok := raw["provider"].(string); ok && strings.TrimSpace(v) != "" {
			provider = strings.ToLower(strings.TrimSpace(v))
		}
	}
	switch provider {
	case "", "s3":
		provider = "s3"
	case "gs", "gcs":
		provider = "gcs"
	case "azure", "file":
		// keep as-is
	default:
		writeHTTPError(w, r, http.StatusBadRequest, "provider must be one of: s3, gcs, azure, file", nil)
		return
	}
	if req.Bucket == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket is required", nil)
		return
	}
	region := strings.TrimSpace(req.Region)
	if provider == "s3" {
		if req.AccessKey == "" || req.SecretKey == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "access_key and secret_key are required for provider=s3", nil)
			return
		}
		if region == "" || strings.TrimSpace(req.Endpoint) == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "region and endpoint are required for provider=s3", nil)
			return
		}
		if strings.Contains(region, "://") || strings.Contains(region, "/") || strings.Contains(region, " ") {
			writeHTTPError(w, r, http.StatusBadRequest, "region must be a plain region name (for example: us-east-1)", nil)
			return
		}
	}
	if strings.TrimSpace(req.Organization) == "" || strings.TrimSpace(req.ProjectId) == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "organization and project_id are required", nil)
		return
	}
	if core.IsGen3Mode(r.Context()) {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if !hasGlobalBucketAdminAccess(r, "create", "update") {
			res := scopeResource(req.Organization, req.ProjectId)
			if res == "" || !hasAnyMethodAccess(r, []string{res}, "create", "update") {
				writeAuthError(w, r)
				return
			}
		}
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
		Provider:  provider,
		Region:    region,
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

func handleInternalDeleteBucket(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface) {
	bucket := mux.Vars(r)["bucket"]
	if bucket == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "bucket is required", nil)
		return
	}
	if core.IsGen3Mode(r.Context()) {
		if !core.HasAuthHeader(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if !hasGlobalBucketAdminAccess(r, "delete") {
			scopes, err := database.ListBucketScopes(r.Context())
			if err != nil {
				writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
				return
			}
			matching := 0
			for _, s := range scopes {
				if s.Bucket != bucket {
					continue
				}
				matching++
				if !hasScopedBucketAccess(r, s, "delete", "update") {
					writeAuthError(w, r)
					return
				}
			}
			// If no scope ties this bucket to a project, require global bucket-admin delete.
			if matching == 0 {
				writeAuthError(w, r)
				return
			}
		}
	}
	if err := database.DeleteS3Credential(r.Context(), bucket); err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
