package internaldrs

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/calypr/drs-server/db/core"
)

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := core.GetRequestID(r.Context())
	if err != nil {
		slog.Error("internal request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("internal request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	code := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		code = http.StatusUnauthorized
	}
	writeHTTPError(w, r, code, "Unauthorized", nil)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, core.ErrUnauthorized):
		writeAuthError(w, r)
	case errors.Is(err, core.ErrConflict):
		writeHTTPError(w, r, http.StatusConflict, err.Error(), err)
	case errors.Is(err, core.ErrNotFound):
		writeHTTPError(w, r, http.StatusNotFound, "Not found", err)
	default:
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
	}
}
