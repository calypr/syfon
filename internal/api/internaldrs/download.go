package internaldrs

import (
	"strconv"
	"time"

	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/api/apiutil"
	"github.com/calypr/syfon/internal/api/attribution"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

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
	if opts.ExpiresIn <= 0 {
		opts.ExpiresIn = time.Duration(config.DefaultSigningExpirySeconds) * time.Second
	}

	signedURL, err := om.SignObjectURL(c.Context(), obj, objectURL, opts)
	if err != nil {
		return apiutil.HandleError(c, err)
	}

	_ = om.RecordDownload(c.Context(), obj.Id)
	attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
		Direction:  models.ProviderTransferDirectionDownload,
		StorageURL: objectURL,
	})

	if c.Query("redirect") == "true" {
		return c.Redirect().To(signedURL)
	}

	return c.JSON(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
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

	opts := urlmanager.SignOptions{
		ExpiresIn: time.Duration(config.DefaultSigningExpirySeconds) * time.Second,
	}

	signedURL, err := om.SignObjectDownloadPart(c.Context(), obj, bucketID, objectURL, start, end, opts)
	if err != nil {
		return apiutil.HandleError(c, err)
	}
	attribution.RecordAccessIssued(c.Context(), om, obj, attribution.AccessDetails{
		Direction:      models.ProviderTransferDirectionDownload,
		StorageURL:     objectURL,
		RangeStart:     &start,
		RangeEnd:       &end,
		BytesRequested: end - start + 1,
	})

	return c.JSON(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
}
