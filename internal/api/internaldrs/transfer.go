package internaldrs

import (
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/api/attribution"
	apimiddleware "github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
)

var multipartUploadSessions sync.Map // uploadID -> multipartSession

func registerInternalTransferRoutes(router fiber.Router, om *core.ObjectManager) {
	router.Get(routeutil.FiberPath(common.RouteInternalDownload), func(c fiber.Ctx) error { return handleInternalDownloadFiber(c, om) })
	router.Get(routeutil.FiberPath(common.RouteInternalDownloadPart), func(c fiber.Ctx) error { return handleInternalDownloadPartFiber(c, om) })
	router.Post(common.RouteInternalUpload, handleInternalUploadBlankFiber(om))
	router.Get(routeutil.FiberPath(common.RouteInternalUploadURL), handleInternalUploadURLFiber(om))
	router.Post(common.RouteInternalUploadBulk, handleInternalUploadBulkFiber(om))
	router.Post(common.RouteInternalMultipartInit, handleInternalMultipartInitFiber(om))
	router.Post(common.RouteInternalMultipartUpload, handleInternalMultipartUploadFiber(om))
	router.Post(common.RouteInternalMultipartComplete, handleInternalMultipartCompleteFiber(om))

	registerInternalBucketRoutes(router, om)
}

func handleInternalDownloadFiber(c fiber.Ctx, om *core.ObjectManager) error {
	fileID := c.Params("file_id")

	obj, err := om.GetObject(c.Context(), fileID, "read")
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	objectURL := core.FirstSupportedAccessURL(obj)
	if objectURL == "" {
		return c.Status(fiber.StatusNotFound).SendString("No supported cloud location found for this file")
	}

	opts := urlmanager.SignOptions{}
	if expStr := c.Query("expires_in"); expStr != "" {
		if exp, err := strconv.Atoi(expStr); err == nil {
			opts.ExpiresIn = time.Duration(exp) * time.Second
		}
	}
	if obj.Name != nil {
		opts.DownloadFilename = common.DownloadFilename(*obj.Name)
	}
	if opts.ExpiresIn <= 0 {
		opts.ExpiresIn = time.Duration(config.DefaultSigningExpirySeconds) * time.Second
	}

	signedURL, err := om.SignObjectURL(c.Context(), obj, objectURL, opts)
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	if err := om.RecordDownload(c.Context(), obj.Id); err != nil {
		return apiutil.HandleError(c, err)
	}
	if err := attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
		Direction:  models.ProviderTransferDirectionDownload,
		StorageURL: objectURL,
	}); err != nil {
		return apiutil.HandleError(c, err)
	}

	if c.Query("redirect") == "true" {
		return c.Redirect().To(signedURL)
	}

	return c.JSON(internalapi.InternalSignedURL{Url: &signedURL})
}

func handleInternalDownloadPartFiber(c fiber.Ctx, om *core.ObjectManager) error {
	fileID := c.Params("file_id")
	startStr := c.Query("start")
	endStr := c.Query("end")

	if startStr == "" || endStr == "" {
		return c.Status(fiber.StatusBadRequest).SendString("Missing 'start' or 'end' query parameter")
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid 'start' parameter")
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return c.Status(fiber.StatusBadRequest).SendString("Invalid 'end' parameter")
	}

	obj, err := om.GetObject(c.Context(), fileID, "read")
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	objectURL := core.FirstSupportedAccessURL(obj)
	if objectURL == "" {
		return c.Status(fiber.StatusNotFound).SendString("No supported cloud location found for this file")
	}

	bucketID := ""
	if b, _, ok := common.ParseS3URL(objectURL); ok {
		bucketID = b
	}

	opts := urlmanager.SignOptions{ExpiresIn: time.Duration(config.DefaultSigningExpirySeconds) * time.Second}
	if obj.Name != nil {
		opts.DownloadFilename = common.DownloadFilename(*obj.Name)
	}
	signedURL, err := om.SignObjectDownloadPart(c.Context(), obj, bucketID, objectURL, start, end, opts)
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	if err := attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
		Direction:      models.ProviderTransferDirectionDownload,
		StorageURL:     objectURL,
		RangeStart:     &start,
		RangeEnd:       &end,
		BytesRequested: end - start + 1,
	}); err != nil {
		return apiutil.HandleError(c, err)
	}

	return c.JSON(internalapi.InternalSignedURL{Url: &signedURL})
}

func handleInternalUploadBlankFiber(om *core.ObjectManager) fiber.Handler {
	return func(c fiber.Ctx) error {
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
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

		bucket, err := om.ResolveBucket(c.Context(), "")
		if err != nil {
			return apiutil.HandleError(c, err)
		}

		key := common.NormalizeUploadKey("", guid)
		urlStr := common.BucketToURL(bucket, key)
		signedURL, err := om.SignURL(c.Context(), urlStr, urlmanager.SignOptions{Method: http.MethodPut})
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
		if apimiddleware.MissingGen3AuthHeader(c.Context()) {
			return c.SendStatus(fiber.StatusUnauthorized)
		}

		fileID := c.Params("file_id")
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
		signedURL, err := om.SignURL(c.Context(), urlStr, urlmanager.SignOptions{Method: http.MethodPut})
		if err != nil {
			return apiutil.HandleError(c, err)
		}
		if obj != nil {
			if err := attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
				Direction:  models.ProviderTransferDirectionUpload,
				StorageURL: urlStr,
			}); err != nil {
				return apiutil.HandleError(c, err)
			}
		}

		return c.JSON(internalapi.InternalSignedURL{Url: &signedURL})
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
			res := internalapi.InternalUploadBulkResult{FileId: item.FileId, FileName: item.FileName}
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
				switch {
				case errors.Is(err, common.ErrUnauthorized):
					res.Status = http.StatusUnauthorized
				case errors.Is(err, common.ErrNotFound):
					res.Status = http.StatusNotFound
				default:
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
			} else if err := attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
				Direction:  models.ProviderTransferDirectionUpload,
				StorageURL: urlStr,
			}); err != nil {
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

		internalID := key
		if common.LooksLikeSHA256(key) {
			if existing, err := om.GetObjectsByChecksum(c.Context(), key, "read"); err == nil && len(existing) > 0 {
				internalID = existing[0].Id
			} else {
				return c.Status(fiber.StatusBadRequest).SendString("checksum-only multipart init requires an explicit guid or a project-scoped object id")
			}
		} else if _, err := uuid.Parse(key); err != nil {
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
