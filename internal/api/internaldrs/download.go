package internaldrs

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
	"github.com/gorilla/mux"
)

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
