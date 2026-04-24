package internaldrs

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

var multipartUploadSessions sync.Map // uploadID -> multipartSession

func handleInternalUploadBlankFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := requireGen3AuthFiber(c); err != nil {
			return err
		}

		var req internalapi.InternalUploadBlankRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		guid := ""
		if req.Guid != nil {
			guid = strings.TrimSpace(*req.Guid)
		}
		if guid == "" {
			guid = uuid.New().String()
		} else if _, err := uuid.Parse(guid); err != nil {
			guid = uuid.New().String()
		}

		// Use guid to find existing or mint new path
		bucket, err := om.ResolveBucket(c.Context(), "")
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		key := common.NormalizeUploadKey("", guid)
		urlStr := common.BucketToURL(bucket, key)

		signedURL, err := om.SignURL(c.Context(), urlStr, urlmanager.SignOptions{
			Method: http.MethodPut,
		})
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.Status(fiber.StatusCreated).JSON(internalapi.InternalUploadBlankOutput{
			Url:    &signedURL,
			Guid:   &guid,
			Bucket: &bucket,
		})
	}
}

func handleInternalUploadURLFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := requireGen3AuthFiber(c); err != nil {
			return err
		}

		fileID := c.Params("file_id")

		// InternalUploadURLParams from query
		var params internalapi.InternalUploadURLParams
		if err := c.Bind().Query(&params); err != nil {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid query parameters")
		}

		obj, err := om.GetObject(c.Context(), fileID, "update")
		if err != nil && !errors.Is(err, common.ErrNotFound) {
			return apiutil.HandleError(c, err)
		}

		bucket, err := om.ResolveBucket(c.Context(), common.StringVal(params.Bucket))
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		key := fileID
		if obj != nil {
			if k, ok := om.ResolveObjectRemotePath(c.Context(), obj.Id, bucket); ok {
				key = k
			}
		}

		urlStr := common.BucketToURL(bucket, key)
		signedURL, err := om.SignURL(c.Context(), urlStr, urlmanager.SignOptions{
			Method: http.MethodPut,
		})
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.JSON(internalapi.InternalSignedURL{
			Url: &signedURL,
		})
	}
}

func handleInternalUploadBulkFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalUploadBulkRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		if len(req.Requests) == 0 {
			empty := []internalapi.InternalUploadBulkResult{}
			return c.JSON(internalapi.InternalUploadBulkOutput{Results: &empty})
		}

		results := make([]internalapi.InternalUploadBulkResult, 0, len(req.Requests))
		for _, item := range req.Requests {
			res := internalapi.InternalUploadBulkResult{
				FileId:   item.FileId,
				FileName: item.FileName,
			}

			if item.FileId == "" {
				errMsg := "FileId is required"
				res.Error = &errMsg
				res.Status = http.StatusBadRequest
				results = append(results, res)
				continue
			}

			obj, err := om.GetObject(c.Context(), item.FileId, "update")
			if err != nil {
				errMsg := err.Error()
				res.Error = &errMsg
				if errors.Is(err, common.ErrUnauthorized) {
					res.Status = http.StatusUnauthorized
				} else if errors.Is(err, common.ErrNotFound) {
					res.Status = http.StatusNotFound
				} else {
					res.Status = http.StatusInternalServerError
				}
				results = append(results, res)
				continue
			}

			bucket, err := om.ResolveBucket(c.Context(), common.StringVal(item.Bucket))
			if err != nil {
				errMsg := err.Error()
				res.Error = &errMsg
				res.Status = http.StatusBadRequest
				results = append(results, res)
				continue
			}

			key, ok := om.ResolveObjectRemotePath(c.Context(), obj.Id, bucket)
			if !ok {
				key = obj.Id
			}

			urlStr := common.BucketToURL(bucket, key)
			signedURL, err := om.SignURL(c.Context(), urlStr, urlmanager.SignOptions{Method: http.MethodPut})
			if err != nil {
				errMsg := err.Error()
				res.Error = &errMsg
				res.Status = http.StatusInternalServerError
			} else {
				res.Url = &signedURL
				res.Bucket = &bucket
				res.FileName = &key
				res.Status = http.StatusOK
			}
			results = append(results, res)
		}

		status := fiber.StatusOK
		for _, r := range results {
			if r.Error != nil {
				status = fiber.StatusMultiStatus
				break
			}
		}

		return c.Status(status).JSON(internalapi.InternalUploadBulkOutput{Results: &results})
	}
}

func handleInternalMultipartInitFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartInitRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}

		bucket, err := om.ResolveBucket(c.Context(), common.StringVal(req.Bucket))
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		key := ""
		if req.Guid != nil {
			key = strings.TrimSpace(*req.Guid)
		} else if req.FileName != nil {
			key = strings.TrimSpace(*req.FileName)
		}

		if key == "" {
			return c.Status(fiber.StatusBadRequest).SendString("key/guid/file_name is required")
		}

		// Path-like keys are already storage keys and should be preserved as-is.
		// Legacy checksum/UUID inputs still use the historic mint/resolve flow.
		if strings.Contains(key, "/") {
			internalID := uuid.NewString()
			uploadID, err := om.InitMultipartUpload(c.Context(), bucket, key)
			if err != nil {
				return apiutil.HandleError(c, err)
			}
			multipartUploadSessions.Store(uploadID, multipartSession{Bucket: bucket, Key: key})
			return c.Status(fiber.StatusOK).JSON(internalapi.InternalMultipartInitOutput{
				UploadId: &uploadID,
				Guid:     &internalID,
			})
		}

		// Syfon internal convention: IDs should be UUIDs.
		// If the provided 'key' looks like a checksum, we mint a UUID from it.
		// If it's already a UUID, we use it. If it's neither, we might need to mint one anyway.
		internalID := key
		if common.LooksLikeSHA256(key) {
			// Resolve existing or mint new UUID from checksum
			if existing, err := om.GetObjectsByChecksum(c.Context(), key); err == nil && len(existing) > 0 {
				internalID = existing[0].Id
			} else {
				internalID = common.MintObjectIDFromChecksum(key, []string{"/data_file"})
				// Pre-register so we don't lose the mapping
				obj := models.InternalObject{
					DrsObject: drs.DrsObject{
						Id:          internalID,
						CreatedTime: time.Now().UTC(),
						Checksums:   []drs.Checksum{{Type: "sha256", Checksum: key}},
					},
				}
				if err := om.RegisterObjects(c.Context(), []models.InternalObject{obj}); err != nil {
					return apiutil.HandleError(c, err)
				}
			}
		} else if _, err := uuid.Parse(key); err != nil {
			// Not a UUID and not a checksum, let's minted a random one or use a stable one?
			// For testing compatibility, we'll mint a random UUID if it's not a UUID.
			internalID = uuid.NewString()
		}

		uploadID, err := om.InitMultipartUpload(c.Context(), bucket, internalID)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		multipartUploadSessions.Store(uploadID, multipartSession{Bucket: bucket, Key: internalID})
		return c.Status(fiber.StatusOK).JSON(internalapi.InternalMultipartInitOutput{
			UploadId: &uploadID,
			Guid:     &internalID,
		})
	}
}

func handleInternalMultipartUploadFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartUploadRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		if req.UploadId == "" {
			return c.Status(fiber.StatusBadRequest).SendString("uploadId is required")
		}

		sess, ok := multipartUploadSessions.Load(req.UploadId)
		if !ok {
			return c.Status(fiber.StatusNotFound).SendString("Upload ID not found")
		}
		s := sess.(multipartSession)

		urlStr, err := om.SignMultipartPart(c.Context(), s.Bucket, s.Key, req.UploadId, req.PartNumber)
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.JSON(internalapi.InternalMultipartUploadOutput{PresignedUrl: &urlStr})
	}
}

func handleInternalMultipartCompleteFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		var req internalapi.InternalMultipartCompleteRequest
		if err := c.Bind().JSON(&req); err != nil && !errors.Is(err, io.EOF) {
			return c.Status(fiber.StatusBadRequest).SendString("Invalid request body")
		}
		if req.UploadId == "" {
			return c.Status(fiber.StatusBadRequest).SendString("uploadId is required")
		}

		sess, ok := multipartUploadSessions.LoadAndDelete(req.UploadId)
		if !ok {
			return c.Status(fiber.StatusNotFound).SendString("Upload ID not found")
		}
		s := sess.(multipartSession)

		parts := make([]urlmanager.MultipartPart, len(req.Parts))
		for i, p := range req.Parts {
			parts[i] = urlmanager.MultipartPart{ETag: p.ETag, PartNumber: p.PartNumber}
		}

		if err := om.CompleteMultipartUpload(c.Context(), s.Bucket, s.Key, req.UploadId, parts); err != nil {
			return apiutil.HandleError(c, err)
		}

		return c.SendStatus(fiber.StatusOK)
	}
}

type multipartSession struct {
	Bucket string
	Key    string
}
