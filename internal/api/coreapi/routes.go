package coreapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	corelogic "github.com/calypr/syfon/internal/coreapi"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/gofiber/fiber/v3"
)

func RegisterCoreRoutes(router fiber.Router, database core.DatabaseInterface) {
	handler := drs.Logger(handleSHA256Validity(database), "CoreSHA256Validity")
	router.Post(routeutil.FiberPath(config.RouteCoreSHA256), routeutil.Handler(handler))
}

func handleSHA256Validity(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkSHA256ValidityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		input := req.Sha256
		if input == nil || len(*input) == 0 {
			input = req.Hashes
		}
		if input == nil || len(*input) == 0 {
			input = &[]string{}
		}

		resp, err := corelogic.ComputeSHA256Validity(r.Context(), database, *input)
		if err != nil {
			status := http.StatusInternalServerError
			msg := err.Error()
			if errors.Is(err, corelogic.ErrNoValidSHA256) {
				status = http.StatusBadRequest
				msg = "No valid sha256 values provided"
			} else {
				slog.Error("coreapi sha256 validity failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
			}
			http.Error(w, msg, status)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Error("coreapi encode response failed", "request_id", core.GetRequestID(r.Context()), "method", r.Method, "path", r.URL.Path, "err", err)
		}
	}
}
