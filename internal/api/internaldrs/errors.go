package internaldrs

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/calypr/syfon/internal/common"
	"github.com/gofiber/fiber/v3"
)

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := common.GetRequestID(r.Context())
	if err != nil {
		slog.Error("internal request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("internal request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}

func writeHTTPErrorFiber(c fiber.Ctx, status int, msg string, err error) error {
	requestID := common.GetRequestID(c.Context())
	if err != nil {
		slog.Error("internal request failed", "request_id", requestID, "method", c.Method(), "path", c.Path(), "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("internal request rejected", "request_id", requestID, "method", c.Method(), "path", c.Path(), "status", status, "msg", msg)
	}
	return c.Status(status).SendString(msg)
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	writeHTTPError(w, r, authStatusCode(r.Context()), "Unauthorized", nil)
}

func writeAuthErrorFiber(c fiber.Ctx) error {
	return writeHTTPErrorFiber(c, authStatusCode(c.Context()), "Unauthorized", nil)
}

func writeDBError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, common.ErrUnauthorized):
		writeAuthError(w, r)
	case errors.Is(err, common.ErrConflict):
		writeHTTPError(w, r, http.StatusConflict, err.Error(), err)
	case errors.Is(err, common.ErrNotFound):
		writeHTTPError(w, r, http.StatusNotFound, "Not found", err)
	default:
		writeHTTPError(w, r, http.StatusInternalServerError, err.Error(), err)
	}
}

func writeDBErrorFiber(c fiber.Ctx, err error) error {
	switch {
	case errors.Is(err, common.ErrUnauthorized):
		return writeAuthErrorFiber(c)
	case errors.Is(err, common.ErrConflict):
		return writeHTTPErrorFiber(c, http.StatusConflict, err.Error(), err)
	case errors.Is(err, common.ErrNotFound):
		return writeHTTPErrorFiber(c, http.StatusNotFound, "Not found", err)
	default:
		return writeHTTPErrorFiber(c, http.StatusInternalServerError, err.Error(), err)
	}
}
