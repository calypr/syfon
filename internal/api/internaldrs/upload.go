package internaldrs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

type multipartSession struct {
	Bucket string
	Key    string
}

var multipartUploadSessions sync.Map // uploadID -> multipartSession

type uploadBulkCredentialCache struct {
	defaultBucket string
	credentials   map[string]models.S3Credential
}

func handleInternalUploadBlankFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalUploadBlankRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request", err)
		}

		guid := ""
		if req.Guid != nil {
			guid = strings.TrimSpace(*req.Guid)
		}
		if guid == "" {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "guid is required", nil)
		}

		targetResources := make([]string, 0)
		if req.Authz != nil {
			targetResources = *req.Authz
		}
		existing, err := service.ResolveObjectByIDOrChecksum(database, c.Context(), guid)
		if err == nil {
			guid = strings.TrimSpace(existing.Id)
			if len(existing.Authorizations) > 0 {
				targetResources = existing.Authorizations
			}
		} else {
			if !errors.Is(err, common.ErrNotFound) {
				return writeDBErrorFiber(c, err)
			}
			// Not found, create blank
			if len(targetResources) == 0 {
				targetResources = []string{"/data_file"}
			}
			if !authz.HasAnyMethodAccess(c.Context(), targetResources, "create", "file_upload") {
				return writeAuthErrorFiber(c)
			}
			if _, parseErr := uuid.Parse(guid); parseErr != nil {
				if common.LooksLikeSHA256(guid) {
					guid = common.MintObjectIDFromChecksum(guid, targetResources)
				} else {
					guid = uuid.NewString()
				}
			}
			now := time.Now()
			obj := &models.InternalObject{
				DrsObject: drs.DrsObject{
					Id:          guid,
					CreatedTime: now,
					UpdatedTime: &now,
					Version:     common.Ptr("1"),
					SelfUri:     "drs://" + guid,
				},
				Authorizations: targetResources,
			}

			if err := database.CreateObject(c.Context(), obj); err != nil {
				return writeDBErrorFiber(c, err)
			}
		}

		// Auth check for existing
		if err == nil && !authz.HasAnyMethodAccess(c.Context(), targetResources, "update", "file_upload") {
			return writeAuthErrorFiber(c)
		}

		creds, err := database.ListS3Credentials(c.Context())
		if err != nil || len(creds) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "No buckets configured for upload", err)
		}
		cred := creds[0]
		objectURL, err := common.ObjectURLForCredential(&cred, guid)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, err.Error(), err)
		}

		if uM == nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "URL Manager not initialized", nil)
		}
		signedURL, err := uM.SignUploadURL(c.Context(), cred.Bucket, objectURL, urlmanager.SignOptions{})
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, err.Error(), err)
		}

		return c.Status(fiber.StatusCreated).JSON(internalapi.InternalUploadBlank201JSONResponse{
			Bucket: &cred.Bucket,
			Guid:   &guid,
			Url:    &signedURL,
		})
	}
}

func handleInternalUploadURLFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		fileID := c.Params("file_id")
		targetResources := []string{"/data_file"}
		if fileID != "" {
			if obj, err := database.GetObject(c.Context(), fileID); err == nil {
				if len(obj.Authorizations) > 0 {
					targetResources = obj.Authorizations
				}
			} else if errors.Is(err, common.ErrUnauthorized) {
				return writeAuthErrorFiber(c)
			}
		}
		if !authz.HasAnyMethodAccess(c.Context(), targetResources, "update", "file_upload") {
			return writeAuthErrorFiber(c)
		}

		bucket := c.Query("bucket")
		fileName := c.Query("filename")

		if bucket == "" {
			creds, _ := database.ListS3Credentials(c.Context())
			if len(creds) > 0 {
				bucket = creds[0].Bucket
			}
		}

		if fileName == "" {
			if resolvedKey, ok := service.ResolveObjectRemotePath(database, c.Context(), fileID, bucket); ok {
				fileName = resolvedKey
			} else {
				fileName = fileID
			}
		}

		if bucket == "" {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "No bucket specified or configured", nil)
		}

		cred, err := database.GetS3Credential(c.Context(), bucket)
		if err != nil {
			return writeDBErrorFiber(c, err)
		}
		objectURL, err := common.ObjectURLForCredential(cred, fileName)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, err.Error(), err)
		}

		opts := urlmanager.SignOptions{}
		if raw := c.Query("expires_in"); raw != "" {
			if exp, err := strconv.Atoi(raw); err == nil {
				opts.ExpiresIn = time.Duration(exp) * time.Second
			}
		}

		if uM == nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "URL Manager not initialized", nil)
		}
		signedURL, err := uM.SignUploadURL(c.Context(), cred.Bucket, objectURL, opts)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, err.Error(), err)
		}

		return c.JSON(internalapi.InternalUploadURL200JSONResponse{Url: &signedURL})
	}
}

func handleInternalUploadBulkFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalUploadBulkRequest
		if err := c.Bind().JSON(&req); err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "invalid request body", err)
		}
		if len(req.Requests) == 0 {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "requests are required", nil)
		}

		results := make([]internalapi.InternalUploadBulkResult, 0, len(req.Requests))
		credCache := uploadBulkCredentialCache{
			credentials: make(map[string]models.S3Credential),
		}
		if creds, err := database.ListS3Credentials(c.Context()); err == nil {
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
			result := signInternalUploadBulkItem(c.Context(), database, uM, item, &credCache)
			results = append(results, result)
		}

		statusCode := fiber.StatusOK
		for _, result := range results {
			if result.Status != int32(http.StatusOK) {
				statusCode = fiber.StatusMultiStatus
				break
			}
		}
		return c.Status(statusCode).JSON(internalapi.InternalUploadBulkOutput{Results: &results})
	}
}

func signInternalUploadBulkItem(ctx context.Context, database db.DatabaseInterface, uM urlmanager.UrlManager, item internalapi.InternalUploadBulkItem, credCache *uploadBulkCredentialCache) internalapi.InternalUploadBulkResult {
	fileID := strings.TrimSpace(item.FileId)
	if fileID == "" {
		return internalapi.InternalUploadBulkResult{Status: int32(http.StatusBadRequest)}
	}

	result := internalapi.InternalUploadBulkResult{
		Status:   int32(http.StatusOK),
		FileId:   fileID,
		FileName: item.FileName,
	}

	bucket := credCache.defaultBucket
	if item.Bucket != nil && strings.TrimSpace(*item.Bucket) != "" {
		bucket = strings.TrimSpace(*item.Bucket)
	}
	if bucket == "" {
		result.Status = int32(http.StatusInternalServerError)
		return result
	}

		if existing, err := service.ResolveObjectByIDOrChecksum(database, ctx, fileID); err == nil {
		if len(existing.Authorizations) > 0 && !authz.HasAnyMethodAccess(ctx, existing.Authorizations, "file_upload", "create", "update") {
			result.Status = int32(http.StatusUnauthorized)
			return result
		}
		if result.FileName == nil || strings.TrimSpace(*result.FileName) == "" {
				if resolvedKey, ok := service.S3KeyFromInternalObjectForBucket(existing, bucket); ok {
				result.FileName = &resolvedKey
			}
		}
	} else if err != nil && !errors.Is(err, common.ErrNotFound) {
		result.Status = int32(http.StatusInternalServerError)
		return result
	}

	cred, ok := credCache.credentials[bucket]
	if !ok {
		c, err := database.GetS3Credential(ctx, bucket)
		if err != nil {
			result.Status = int32(http.StatusInternalServerError)
			return result
		}
		cred = *c
		credCache.credentials[bucket] = cred
	}

	objectURL, err := common.ObjectURLForCredential(&cred, fileID)
	if err != nil {
		result.Status = int32(http.StatusBadRequest)
		return result
	}

	opts := urlmanager.SignOptions{}
	if item.ExpiresIn != nil && *item.ExpiresIn > 0 {
		opts.ExpiresIn = time.Duration(*item.ExpiresIn) * time.Second
	}

	signedURL, err := uM.SignUploadURL(ctx, bucket, objectURL, opts)
	if err != nil {
		result.Status = int32(http.StatusInternalServerError)
		return result
	}

	result.Bucket = &bucket
	result.Url = &signedURL
	return result
}

func handleInternalMultipartInitFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartInitRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request", err)
		}

		candidate := ""
		if req.Guid != nil {
			candidate = strings.TrimSpace(*req.Guid)
		}
		if candidate == "" && req.FileName != nil {
			candidate = strings.TrimSpace(*req.FileName)
		}
		if candidate == "" {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "guid or file_name is required", nil)
		}

		guid := candidate
		checksum := ""
		if common.LooksLikeSHA256(candidate) {
			checksum = candidate
		if existing, err := service.ResolveObjectByIDOrChecksum(database, c.Context(), candidate); err == nil && existing != nil {
				guid = strings.TrimSpace(existing.Id)
			} else {
				guid = uuid.NewString()
				now := time.Now().UTC()
				obj := &models.InternalObject{
					DrsObject: drs.DrsObject{
						Id:          guid,
						CreatedTime: now,
						UpdatedTime: &now,
						Version:     common.Ptr("1"),
						SelfUri:     "drs://" + guid,
						Checksums:   []drs.Checksum{{Type: "sha256", Checksum: checksum}},
					},
					Authorizations: []string{"/data_file"},
				}
				if err := database.CreateObject(c.Context(), obj); err != nil {
					return writeDBErrorFiber(c, err)
				}
			}
		} else {
			if _, err := uuid.Parse(candidate); err != nil {
				guid = uuid.NewString()
			}
		}

		targetResources := []string{"/data_file"}
		if obj, err := database.GetObject(c.Context(), guid); err == nil && len(obj.Authorizations) > 0 {
			targetResources = obj.Authorizations
		}
		if !authz.HasAnyMethodAccess(c.Context(), targetResources, "create", "file_upload", "update") {
			return writeAuthErrorFiber(c)
		}

		bucket, err := service.ResolveBucket(c.Context(), database, common.StringVal(req.Bucket))
		if err != nil || bucket == "" {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "No bucket configured", err)
		}

		uploadID, err := uM.InitMultipartUpload(c.Context(), bucket, guid)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, err.Error(), err)
		}

		multipartUploadSessions.Store(uploadID, multipartSession{Bucket: bucket, Key: guid})

		return c.Status(fiber.StatusCreated).JSON(internalapi.InternalMultipartInitOutput{
			Guid:     &guid,
			UploadId: &uploadID,
		})
	}
}

func handleInternalMultipartUploadFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartUploadRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request", err)
		}
		if req.Key == "" {
			req.Key = c.Query("key")
		}
		if req.UploadId == "" {
			req.UploadId = c.Query("uploadId")
		}
		if req.PartNumber <= 0 {
			if raw := c.Query("partNumber"); raw != "" {
				if v, err := strconv.ParseInt(raw, 10, 32); err == nil && v > 0 && v <= math.MaxInt32 {
					req.PartNumber = int32(v)
				}
			}
		}
		if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
			if b := c.Query("bucket"); b != "" {
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
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "key, uploadId, and positive partNumber are required", nil)
		}
		targetResources := []string{"/data_file"}
		if req.Key != "" {
			if obj, err := database.GetObject(c.Context(), req.Key); err == nil && len(obj.Authorizations) > 0 {
				targetResources = obj.Authorizations
			} else if err != nil && !errors.Is(err, common.ErrNotFound) {
				return writeDBErrorFiber(c, err)
			}
		}
		if !authz.HasAnyMethodAccess(c.Context(), targetResources, "file_upload", "create", "update") {
			return writeAuthErrorFiber(c)
		}

		bucket, err := service.ResolveBucket(c.Context(), database, common.StringVal(req.Bucket))
		if err != nil || bucket == "" {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "No bucket configured", err)
		}

		signedURL, err := uM.SignMultipartPart(c.Context(), bucket, req.Key, req.UploadId, req.PartNumber)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, err.Error(), err)
		}

		return c.JSON(internalapi.InternalMultipartUploadOutput{PresignedUrl: &signedURL})
	}
}

func handleInternalMultipartCompleteFiber(database db.DatabaseInterface, uM urlmanager.UrlManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartCompleteRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "Invalid request", err)
		}
		if req.Key == "" {
			req.Key = c.Query("key")
		}
		if req.UploadId == "" {
			req.UploadId = c.Query("uploadId")
		}
		if req.Bucket == nil || strings.TrimSpace(*req.Bucket) == "" {
			if b := c.Query("bucket"); b != "" {
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
			return writeHTTPErrorFiber(c, fiber.StatusBadRequest, "key and uploadId are required", nil)
		}
		targetResources := []string{"/data_file"}
		if req.Key != "" {
			if obj, err := database.GetObject(c.Context(), req.Key); err == nil && len(obj.Authorizations) > 0 {
				targetResources = obj.Authorizations
			} else if err != nil && !errors.Is(err, common.ErrNotFound) {
				return writeDBErrorFiber(c, err)
			}
		}
		if !authz.HasAnyMethodAccess(c.Context(), targetResources, "file_upload", "create", "update") {
			return writeAuthErrorFiber(c)
		}

		bucket, err := service.ResolveBucket(c.Context(), database, common.StringVal(req.Bucket))
		if err != nil || bucket == "" {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, "No bucket configured", err)
		}

		var parts []urlmanager.MultipartPart
		for _, p := range req.Parts {
			parts = append(parts, urlmanager.MultipartPart{
				PartNumber: p.PartNumber,
				ETag:       p.ETag,
			})
		}

		err = uM.CompleteMultipartUpload(c.Context(), bucket, req.Key, req.UploadId, parts)
		if err != nil {
			return writeHTTPErrorFiber(c, fiber.StatusInternalServerError, err.Error(), err)
		}
		multipartUploadSessions.Delete(req.UploadId)
		if recErr := database.RecordFileUpload(c.Context(), req.Key); recErr != nil {
			// ignore metric error
		}

		return c.SendStatus(fiber.StatusOK)
	}
}
