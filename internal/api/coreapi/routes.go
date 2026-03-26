package coreapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	corelogic "github.com/calypr/drs-server/internal/coreapi"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/apigen/internalapi"
	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/gorilla/mux"
)

func RegisterCoreRoutes(router *mux.Router, database core.DatabaseInterface) {
	handler := drs.Logger(handleSHA256Validity(database), "CoreSHA256Validity")
	router.Handle(config.RouteCoreSHA256, handler).Methods(http.MethodPost)
}

func handleSHA256Validity(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req internalapi.BulkSHA256ValidityRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		input := req.Sha256
		if len(input) == 0 {
			input = req.Hashes
		}

		resp, err := corelogic.ComputeSHA256Validity(r.Context(), database, input)
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
