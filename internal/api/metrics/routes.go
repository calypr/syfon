package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/calypr/drs-server/db/core"
	"github.com/gorilla/mux"
)

type listResponse struct {
	Data   []core.FileUsage `json:"data"`
	Limit  int              `json:"limit"`
	Offset int              `json:"offset"`
}

func RegisterMetricsRoutes(router *mux.Router, database core.DatabaseInterface) {
	router.HandleFunc("/internal/metrics/files", handleListFileUsage(database)).Methods(http.MethodGet)
	router.HandleFunc("/internal/v1/metrics/files", handleListFileUsage(database)).Methods(http.MethodGet)
	router.HandleFunc("/internal/metrics/files/{object_id}", handleGetFileUsage(database)).Methods(http.MethodGet)
	router.HandleFunc("/internal/v1/metrics/files/{object_id}", handleGetFileUsage(database)).Methods(http.MethodGet)
	router.HandleFunc("/internal/metrics/summary", handleGetSummary(database)).Methods(http.MethodGet)
	router.HandleFunc("/internal/v1/metrics/summary", handleGetSummary(database)).Methods(http.MethodGet)
}

func handleListFileUsage(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !hasMetricsReadAccess(r.Context()) {
			writeAuthError(w, r)
			return
		}
		limit := parseIntQuery(r, "limit", 200)
		offset := parseIntQuery(r, "offset", 0)
		if limit < 1 || limit > 1000 || offset < 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "invalid pagination params", nil)
			return
		}
		inactiveSince, err := parseInactiveSince(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		data, err := database.ListFileUsage(r.Context(), limit, offset, inactiveSince)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to list file usage", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(listResponse{Data: data, Limit: limit, Offset: offset}); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func handleGetFileUsage(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !hasMetricsReadAccess(r.Context()) {
			writeAuthError(w, r)
			return
		}
		objectID := mux.Vars(r)["object_id"]
		if objectID == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "object_id is required", nil)
			return
		}
		usage, err := database.GetFileUsage(r.Context(), objectID)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				writeHTTPError(w, r, http.StatusNotFound, "file usage not found", err)
				return
			}
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to get file usage", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(usage); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func handleGetSummary(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !hasMetricsReadAccess(r.Context()) {
			writeAuthError(w, r)
			return
		}
		inactiveSince, err := parseInactiveSince(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		summary, err := database.GetFileUsageSummary(r.Context(), inactiveSince)
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to get file usage summary", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(summary); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func parseInactiveSince(r *http.Request) (*time.Time, error) {
	raw := r.URL.Query().Get("inactive_days")
	if raw == "" {
		return nil, nil
	}
	days, err := strconv.Atoi(raw)
	if err != nil || days < 0 {
		return nil, errors.New("inactive_days must be a non-negative integer")
	}
	t := time.Now().UTC().AddDate(0, 0, -days)
	return &t, nil
}

func parseIntQuery(r *http.Request, key string, defaultValue int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return defaultValue
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return -1
	}
	return v
}

func hasMetricsReadAccess(ctx context.Context) bool {
	// In local mode this always returns true.
	return core.HasMethodAccess(ctx, "read", []string{"/data_file"})
}

func writeAuthError(w http.ResponseWriter, r *http.Request) {
	status := http.StatusForbidden
	if core.IsGen3Mode(r.Context()) && !core.HasAuthHeader(r.Context()) {
		status = http.StatusUnauthorized
	}
	writeHTTPError(w, r, status, "Unauthorized", nil)
}

func writeHTTPError(w http.ResponseWriter, r *http.Request, status int, msg string, err error) {
	requestID := core.GetRequestID(r.Context())
	if err != nil {
		slog.Error("metrics request failed", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg, "err", err)
	} else {
		slog.Warn("metrics request rejected", "request_id", requestID, "method", r.Method, "path", r.URL.Path, "status", status, "msg", msg)
	}
	http.Error(w, msg, status)
}
