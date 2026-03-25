package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/metricsapi"
	"github.com/calypr/drs-server/db/core"
	"github.com/gorilla/mux"
)

func RegisterMetricsRoutes(router *mux.Router, database core.DatabaseInterface) {
	listHandler := handleListFileUsage(database)
	getHandler := handleGetFileUsage(database)
	summaryHandler := handleGetSummary(database)

	router.HandleFunc("/index/v1/metrics/files", listHandler).Methods(http.MethodGet)
	router.HandleFunc("/index/v1/metrics/files/{object_id}", getHandler).Methods(http.MethodGet)
	router.HandleFunc("/index/v1/metrics/summary", summaryHandler).Methods(http.MethodGet)
}

func handleListFileUsage(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		access, err := resolveMetricsAccess(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
			return
		}
		if !access.authorized(r.Context()) {
			writeAuthError(w, r)
			return
		}

		var data []core.FileUsage
		if access.isScoped() {
			data, _, err = listScopedFileUsage(r.Context(), database, access.scopePrefix, limit, offset, inactiveSince)
		} else {
			data, err = database.ListFileUsage(r.Context(), limit, offset, inactiveSince)
		}
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to list file usage", err)
			return
		}
		out := metricsapi.NewMetricsListResponse()
		items := make([]metricsapi.FileUsage, 0, len(data))
		for _, v := range data {
			items = append(items, toMetricsFileUsage(v))
		}
		out.SetData(items)
		out.SetLimit(int32(limit))
		out.SetOffset(int32(offset))
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func handleGetFileUsage(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		objectID := mux.Vars(r)["object_id"]
		if objectID == "" {
			writeHTTPError(w, r, http.StatusBadRequest, "object_id is required", nil)
			return
		}

		access, err := resolveMetricsAccess(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
			return
		}
		if !access.authorized(r.Context()) {
			writeAuthError(w, r)
			return
		}
		if access.isScoped() {
			inside, err := objectInScope(r.Context(), database, objectID, access.scopePrefix)
			if err != nil {
				writeHTTPError(w, r, http.StatusInternalServerError, "failed to evaluate object scope", err)
				return
			}
			if !inside {
				writeHTTPError(w, r, http.StatusNotFound, "file usage not found", core.ErrNotFound)
				return
			}
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
		if err := json.NewEncoder(w).Encode(toMetricsFileUsage(*usage)); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func handleGetSummary(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		inactiveSince, err := parseInactiveSince(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), nil)
			return
		}
		access, err := resolveMetricsAccess(r)
		if err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, err.Error(), err)
			return
		}
		if !access.authorized(r.Context()) {
			writeAuthError(w, r)
			return
		}

		var summary core.FileUsageSummary
		if access.isScoped() {
			_, summary, err = listScopedFileUsage(r.Context(), database, access.scopePrefix, 0, 0, inactiveSince)
		} else {
			summary, err = database.GetFileUsageSummary(r.Context(), inactiveSince)
		}
		if err != nil {
			writeHTTPError(w, r, http.StatusInternalServerError, "failed to get file usage summary", err)
			return
		}
		out := metricsapi.NewFileUsageSummary()
		out.SetTotalFiles(summary.TotalFiles)
		out.SetTotalUploads(summary.TotalUploads)
		out.SetTotalDownloads(summary.TotalDownloads)
		out.SetInactiveFileCount(summary.InactiveFileCount)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(out); err != nil {
			slog.Error("metrics encode response failed", "request_id", core.GetRequestID(r.Context()), "path", r.URL.Path, "err", err)
		}
	}
}

func toMetricsFileUsage(v core.FileUsage) metricsapi.FileUsage {
	out := metricsapi.NewFileUsage()
	out.SetObjectId(v.ObjectID)
	out.SetName(v.Name)
	out.SetSize(v.Size)
	out.SetUploadCount(v.UploadCount)
	out.SetDownloadCount(v.DownloadCount)
	if v.LastUploadTime != nil {
		out.SetLastUploadTime(*v.LastUploadTime)
	}
	if v.LastDownloadTime != nil {
		out.SetLastDownloadTime(*v.LastDownloadTime)
	}
	if v.LastAccessTime != nil {
		out.SetLastAccessTime(*v.LastAccessTime)
	}
	return *out
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

type metricsAccess struct {
	scopePrefix string
}

func (a metricsAccess) isScoped() bool {
	return strings.TrimSpace(a.scopePrefix) != ""
}

func (a metricsAccess) authorized(ctx context.Context) bool {
	if a.isScoped() {
		return hasMetricsReadAccess(ctx, a.scopePrefix)
	}
	return hasGlobalMetricsReadAccess(ctx)
}

func resolveMetricsAccess(r *http.Request) (metricsAccess, error) {
	scopePrefix, _, err := parseScopeQuery(r)
	if err != nil {
		return metricsAccess{}, err
	}
	return metricsAccess{scopePrefix: scopePrefix}, nil
}

func hasMetricsReadAccess(ctx context.Context, resource string) bool {
	if strings.TrimSpace(resource) == "" {
		return hasGlobalMetricsReadAccess(ctx)
	}
	return core.HasMethodAccess(ctx, "read", []string{resource})
}

func hasGlobalMetricsReadAccess(ctx context.Context) bool {
	// In local mode HasMethodAccess always returns true.
	// In Gen3 mode, this allows existing "/data_file" readers and indexd admins
	// that have read access at program scope.
	return core.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		core.HasMethodAccess(ctx, "read", []string{"/programs"})
}

func parseScopeQuery(r *http.Request) (string, bool, error) {
	authz := strings.TrimSpace(r.URL.Query().Get("authz"))
	if authz != "" {
		return authz, true, nil
	}
	org := strings.TrimSpace(r.URL.Query().Get("organization"))
	if org == "" {
		org = strings.TrimSpace(r.URL.Query().Get("program"))
	}
	project := strings.TrimSpace(r.URL.Query().Get("project"))
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := core.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func collectScopedUsage(ctx context.Context, database core.DatabaseInterface, scopePrefix string, inactiveSince *time.Time) ([]core.FileUsage, core.FileUsageSummary, error) {
	ids, err := database.ListObjectIDsByResourcePrefix(ctx, scopePrefix)
	if err != nil {
		return nil, core.FileUsageSummary{}, err
	}
	sort.Strings(ids)

	summary := core.FileUsageSummary{TotalFiles: int64(len(ids))}
	usages := make([]core.FileUsage, 0, len(ids))
	for _, id := range ids {
		usage, err := database.GetFileUsage(ctx, id)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				if inactiveSince != nil {
					summary.InactiveFileCount++
				}
				obj, objErr := database.GetObject(ctx, id)
				if objErr != nil {
					if errors.Is(objErr, core.ErrNotFound) {
						continue
					}
					return nil, core.FileUsageSummary{}, objErr
				}
				usages = append(usages, core.FileUsage{
					ObjectID: id,
					Name:     obj.Name,
					Size:     obj.Size,
				})
				continue
			}
			return nil, core.FileUsageSummary{}, err
		}
		summary.TotalUploads += usage.UploadCount
		summary.TotalDownloads += usage.DownloadCount
		if inactiveSince != nil && (usage.LastDownloadTime == nil || usage.LastDownloadTime.Before(*inactiveSince)) {
			summary.InactiveFileCount++
		}
		if inactiveSince != nil && usage.LastDownloadTime != nil && !usage.LastDownloadTime.Before(*inactiveSince) {
			continue
		}
		usages = append(usages, *usage)
	}
	return usages, summary, nil
}

func listScopedFileUsage(ctx context.Context, database core.DatabaseInterface, scopePrefix string, limit, offset int, inactiveSince *time.Time) ([]core.FileUsage, core.FileUsageSummary, error) {
	usages, summary, err := collectScopedUsage(ctx, database, scopePrefix, inactiveSince)
	if err != nil {
		return nil, core.FileUsageSummary{}, err
	}
	if limit <= 0 {
		return usages, summary, nil
	}
	if offset >= len(usages) {
		return []core.FileUsage{}, summary, nil
	}
	end := offset + limit
	if end > len(usages) {
		end = len(usages)
	}
	return usages[offset:end], summary, nil
}

func objectInScope(ctx context.Context, database core.DatabaseInterface, objectID, scopePrefix string) (bool, error) {
	obj, err := database.GetObject(ctx, objectID)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	for _, authz := range obj.Authorizations {
		if authz == scopePrefix || strings.HasPrefix(authz, scopePrefix+"/") {
			return true, nil
		}
	}
	return false, nil
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
