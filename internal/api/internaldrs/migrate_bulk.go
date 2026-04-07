package internaldrs

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

// migrateBulkRecord is the wire format for POST /index/migrate/bulk.
// It carries the full DRS-native representation of a single migrated object,
// including the original source ID, all timestamps, version, description,
// checksums, access methods and authz — fields that the standard
// /index/bulk path cannot preserve.
type migrateBulkRecord struct {
	ID            string             `json:"id"`
	Name          string             `json:"name,omitempty"`
	Size          int64              `json:"size"`
	Version       string             `json:"version,omitempty"`
	Description   string             `json:"description,omitempty"`
	CreatedTime   time.Time          `json:"created_time"`
	UpdatedTime   time.Time          `json:"updated_time,omitempty"`
	Checksums     []drs.Checksum     `json:"checksums"`
	AccessMethods []drs.AccessMethod `json:"access_methods,omitempty"`
	Authz         []string           `json:"authz,omitempty"`
}

type migrateBulkRequest struct {
	Records []migrateBulkRecord `json:"records"`
}

type migrateBulkResponse struct {
	Count int `json:"count"`
}

// handleMigrateBulk accepts a batch of fully-specified DRS objects and
// persists them via database.RegisterObjects, which performs an upsert that
// preserves every field: original ID (not reminted), source timestamps,
// version, description, checksums, access methods and authz.
//
// This is the only Syfon endpoint intended for Indexd migration use.
// Normal ingestion paths should use /index or /index/bulk.
func handleMigrateBulk(database core.DatabaseInterface) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req migrateBulkRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeHTTPError(w, r, http.StatusBadRequest, "invalid request body", nil)
			return
		}
		if len(req.Records) == 0 {
			writeHTTPError(w, r, http.StatusBadRequest, "records cannot be empty", nil)
			return
		}

		objects := make([]core.InternalObject, 0, len(req.Records))
		for _, rec := range req.Records {
			id := strings.TrimSpace(rec.ID)
			if id == "" {
				writeHTTPError(w, r, http.StatusBadRequest, "record missing id", nil)
				return
			}

			now := time.Now().UTC()
			createdTime := rec.CreatedTime
			if createdTime.IsZero() {
				createdTime = now
			}
			updatedTime := rec.UpdatedTime
			if updatedTime.IsZero() {
				updatedTime = createdTime
			}

			obj := core.InternalObject{
				DrsObject: drs.DrsObject{
					Id:            id,
					SelfUri:       "drs://" + id,
					Name:          rec.Name,
					Size:          rec.Size,
					Version:       rec.Version,
					Description:   rec.Description,
					CreatedTime:   createdTime,
					UpdatedTime:   updatedTime,
					Checksums:     append([]drs.Checksum(nil), rec.Checksums...),
					AccessMethods: append([]drs.AccessMethod(nil), rec.AccessMethods...),
				},
				Authorizations: append([]string(nil), rec.Authz...),
			}
			objects = append(objects, obj)
		}

		if err := database.RegisterObjects(r.Context(), objects); err != nil {
			slog.Error("migrate bulk: RegisterObjects failed",
				"request_id", core.GetRequestID(r.Context()),
				"count", len(objects),
				"err", err,
			)
			writeDBError(w, r, err)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(migrateBulkResponse{Count: len(objects)}); err != nil {
			slog.Error("migrate bulk: encode response failed",
				"request_id", core.GetRequestID(r.Context()),
				"err", err,
			)
		}
	}
}

