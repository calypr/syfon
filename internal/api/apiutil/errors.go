package apiutil

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/gofiber/fiber/v3"
)

// HandleError maps core/domain errors to standardized Fiber HTTP responses.
// It handles logging and request ID extraction automatically.
func HandleError(c fiber.Ctx, err error) error {
	if err == nil {
		return nil
	}

	status := http.StatusInternalServerError
	msg := err.Error()

	switch {
	case errors.Is(err, common.ErrNotFound):
		status = http.StatusNotFound
		msg = "Resource not found"
	case errors.Is(err, common.ErrUnauthorized):
		status = http.StatusForbidden
		if authz.IsGen3Mode(c.Context()) && !authz.HasAuthHeader(c.Context()) {
			status = http.StatusUnauthorized
		}
		msg = "Unauthorized"
		var publicErr common.PublicError
		if status == http.StatusForbidden && errors.As(err, &publicErr) {
			msg = publicErr.PublicMessage()
		}
	case errors.Is(err, common.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, common.ErrNoValidSHA256):
		status = http.StatusBadRequest
		msg = "A valid SHA256 checksum is required"
	}

	requestID := common.GetRequestID(c.Context())
	if status >= 500 {
		slog.Error("request failed", "request_id", requestID, "method", c.Method(), "path", c.Path(), "status", status, "err", err)
	} else {
		slog.Warn("request rejected", "request_id", requestID, "method", c.Method(), "path", c.Path(), "status", status, "msg", msg, "err", err)
	}

	return c.Status(status).SendString(msg)
}
