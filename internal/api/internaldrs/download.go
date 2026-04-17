package internaldrs

import (
	"github.com/calypr/syfon/internal/db"

	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/internalapi"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

func isSafeRedirectTarget(raw string) bool {
	target := strings.TrimSpace(raw)
	if target == "" {
		return false
	}

	u, err := url.Parse(target)
	if err != nil {
		return false
	}

	// Absolute URLs are allowed only for http/https.
	if u.IsAbs() {
		s := strings.ToLower(u.Scheme)
		return s == "http" || s == "https"
	}

	// Relative redirect must be absolute-path on same host, but reject // and /\.
	return strings.HasPrefix(target, "/") &&
		!(len(target) > 1 && (target[1] == '/' || target[1] == '\\'))
}

func handleInternalDownloadFiber(c fiber.Ctx, database db.DatabaseInterface, uM urlmanager.UrlManager) error {
	fileID := c.Params("file_id")

	obj, err := service.ResolveObjectByIDOrChecksum(database, c.Context(), fileID)
	if err != nil {
		return writeDBErrorFiber(c, err)
	}
	if !authz.HasAnyMethodAccess(c.Context(), obj.Authorizations, "read") {
		return writeAuthErrorFiber(c)
	}

	objectURL := service.FirstSupportedAccessURL(obj)

	if objectURL == "" {
		return writeHTTPErrorFiber(c, http.StatusNotFound, "No supported cloud location found for this file", nil)
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

	bucketID := ""
	if parsed, parseErr := url.Parse(objectURL); parseErr == nil {
		bucketID = parsed.Host
	}

	signedURL, err := uM.SignURL(c.Context(), bucketID, objectURL, opts)
	if err != nil {
		return writeHTTPErrorFiber(c, http.StatusInternalServerError, err.Error(), err)
	}

	if recErr := database.RecordFileDownload(c.Context(), obj.Id); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", common.GetRequestID(c.Context()), "file_id", obj.Id, "err", recErr)
	}

	if c.Query("redirect") == "true" {
		if !isSafeRedirectTarget(signedURL) {
			return writeHTTPErrorFiber(c, http.StatusBadRequest, "Unsafe redirect URL", nil)
		}
		return c.Redirect().To(signedURL)
	}

	return c.JSON(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
}

func handleInternalDownloadPartFiber(c fiber.Ctx, database db.DatabaseInterface, uM urlmanager.UrlManager) error {
	fileID := c.Params("file_id")

	startStr := c.Query("start")
	endStr := c.Query("end")

	if startStr == "" || endStr == "" {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "Missing 'start' or 'end' query parameter", nil)
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "Invalid 'start' parameter", err)
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return writeHTTPErrorFiber(c, http.StatusBadRequest, "Invalid 'end' parameter", err)
	}

	obj, err := service.ResolveObjectByIDOrChecksum(database, c.Context(), fileID)
	if err != nil {
		return writeDBErrorFiber(c, err)
	}
	if !authz.HasAnyMethodAccess(c.Context(), obj.Authorizations, "read") {
		return writeAuthErrorFiber(c)
	}

	objectURL := service.FirstSupportedAccessURL(obj)

	if objectURL == "" {
		return writeHTTPErrorFiber(c, http.StatusNotFound, "No supported cloud location found for this file", nil)
	}

	bucketID := ""
	if parsed, parseErr := url.Parse(objectURL); parseErr == nil {
		bucketID = parsed.Host
	}

	opts := urlmanager.SignOptions{
		ExpiresIn: time.Duration(config.DefaultSigningExpirySeconds) * time.Second,
	}

	signedURL, err := uM.SignDownloadPart(c.Context(), bucketID, objectURL, start, end, opts)
	if err != nil {
		return writeHTTPErrorFiber(c, http.StatusInternalServerError, err.Error(), err)
	}

	return c.JSON(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
}
