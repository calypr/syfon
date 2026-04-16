package internaldrs

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/provider"
	"github.com/calypr/syfon/internal/urlmanager"
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

func handleInternalDownload(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	fileID := routeutil.PathParam(r, "file_id")

	obj, err := resolveObjectByIDOrChecksum(database, r.Context(), fileID)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if !hasAnyMethodAccess(r, obj.Authorizations, "read") {
		writeAuthError(w, r)
		return
	}

	// Find first supported access method (s3, gs, azblob)
	var objectURL string
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			u, err := url.Parse(am.AccessUrl.Url)
			if err != nil {
				continue
			}
			if provider.FromScheme(u.Scheme) != "" {
				objectURL = am.AccessUrl.Url
				break
			}
		}
	}

	if objectURL == "" {
		writeHTTPError(w, r, http.StatusNotFound, "No supported cloud location found for this file", nil)
		return
	}

	opts := urlmanager.SignOptions{}
	if expStr := r.URL.Query().Get("expires_in"); expStr != "" {
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

	signedURL, err := uM.SignURL(r.Context(), bucketID, objectURL, opts)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	if recErr := database.RecordFileDownload(r.Context(), obj.Id); recErr != nil {
		slog.Debug("failed to record file download metric", "request_id", core.GetRequestID(r.Context()), "file_id", obj.Id, "err", recErr)
	}

	if r.URL.Query().Get("redirect") == "true" {
		if !isSafeRedirectTarget(signedURL) {
			writeHTTPError(w, r, http.StatusBadRequest, "Unsafe redirect URL", nil)
			return
		}
		http.Redirect(w, r, signedURL, http.StatusFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
}

func handleInternalDownloadPart(w http.ResponseWriter, r *http.Request, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	fileID := routeutil.PathParam(r, "file_id")

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	if startStr == "" || endStr == "" {
		writeHTTPError(w, r, http.StatusBadRequest, "Missing 'start' or 'end' query parameter", nil)
		return
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid 'start' parameter", err)
		return
	}
	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		writeHTTPError(w, r, http.StatusBadRequest, "Invalid 'end' parameter", err)
		return
	}

	obj, err := resolveObjectByIDOrChecksum(database, r.Context(), fileID)
	if err != nil {
		writeDBError(w, r, err)
		return
	}
	if !hasAnyMethodAccess(r, obj.Authorizations, "read") {
		writeAuthError(w, r)
		return
	}

	// Find first supported access method (s3, gs, azblob)
	var objectURL string
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl.Url == "" {
				continue
			}
			u, err := url.Parse(am.AccessUrl.Url)
			if err != nil {
				continue
			}
			if provider.FromScheme(u.Scheme) != "" {
				objectURL = am.AccessUrl.Url
				break
			}
		}
	}

	if objectURL == "" {
		writeHTTPError(w, r, http.StatusNotFound, "No supported cloud location found for this file", nil)
		return
	}

	bucketID := ""
	if parsed, parseErr := url.Parse(objectURL); parseErr == nil {
		bucketID = parsed.Host
	}

	opts := urlmanager.SignOptions{
		ExpiresIn: time.Duration(config.DefaultSigningExpirySeconds) * time.Second,
	}

	signedURL, err := uM.SignDownloadPart(r.Context(), bucketID, objectURL, start, end, opts)
	if err != nil {
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(internalapi.InternalSignedURL{
		Url: &signedURL,
	})
}
