package migrate

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

// Config holds the configuration for a single migration run.
type Config struct {
	// IndexdURL is the base URL of the source Indexd (or Syfon-compat) server,
	// e.g. "https://indexd.example.org".
	IndexdURL string

	// SyfonURL is the base URL of the target Syfon DRS server,
	// e.g. "http://localhost:8080".
	SyfonURL string

	// BatchSize controls how many records are fetched and written per round
	// trip.  Defaults to 100 when zero.
	BatchSize int

	// Limit caps the total number of records migrated.  Zero means unlimited.
	Limit int

	// DryRun fetches and transforms records but does not write to Syfon.
	DryRun bool

	// DefaultAuthz is appended to any record that has an empty authz list.
	DefaultAuthz []string
}

// Stats summarises the outcome of a migration run.
type Stats struct {
	Fetched     int
	Transformed int
	Loaded      int
	Skipped     int
	Errors      int
}

func (s Stats) String() string {
	return fmt.Sprintf(
		"fetched=%d transformed=%d loaded=%d skipped=%d errors=%d",
		s.Fetched, s.Transformed, s.Loaded, s.Skipped, s.Errors,
	)
}

// migrateWireRecord is the JSON payload sent to POST /index/migrate/bulk.
// It must match the migrateBulkRecord struct in
// internal/api/internaldrs/migrate_bulk.go field-for-field.
type migrateWireRecord struct {
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

type migrateWireRequest struct {
	Records []migrateWireRecord `json:"records"`
}

// Run executes the full Indexd → Syfon ETL pipeline:
//
//  1. Extract   – paginate records from the Indexd API
//  2. Transform – apply the DRS field mapping (issue #20)
//  3. Validate  – checksums and id must be present
//  4. Load      – POST /index/migrate/bulk, which calls RegisterObjects
//                 directly to preserve all fields including original ID,
//                 timestamps, version and description
//
// The pipeline is idempotent: RegisterObjects upserts records, so re-running
// is safe.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	src := NewIndexdClient(cfg.IndexdURL)
	var httpClient *http.Client
	if !cfg.DryRun {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}

	var (
		stats        Stats
		cursorStart  string // empty = first request; updated each round
		pageNum      int    // only used in page mode
		cursorMode   bool   // true once source emits a non-empty cursor
	)

	for {
		// Gate: stop starting new fetches once the limit is reached.
		if cfg.Limit > 0 && stats.Fetched >= cfg.Limit {
			break
		}

		// Always request a full batchSize.  The limit only controls whether
		// we start a new fetch, never how many records we consume from one
		// already-fetched page (to avoid data-loss with cursor-based sources).
		records, nextStart, err := src.ListPage(ctx, batchSize, cursorStart, pageNum)
		if err != nil {
			return stats, fmt.Errorf("fetch page (cursor=%q page=%d): %w", cursorStart, pageNum, err)
		}
		if len(records) == 0 {
			break // source exhausted
		}

		// Detect pagination mode on the first response that carries a cursor.
		if nextStart != "" {
			cursorMode = true
		}

		stats.Fetched += len(records)
		slog.Info("migrate: fetched", "count", len(records), "cursor", cursorStart, "page", pageNum)

		// Apply default authz to records that arrive without one.
		if len(cfg.DefaultAuthz) > 0 {
			for i := range records {
				if len(records[i].Authz) == 0 {
					records[i].Authz = append([]string(nil), cfg.DefaultAuthz...)
				}
			}
		}

		objects, transformErrs := TransformBatch(records)
		for _, te := range transformErrs {
			slog.Warn("migrate: transform error", "did", te.DID, "err", te.Err)
			stats.Errors++
		}
		stats.Transformed += len(objects)

		// Validate each transformed object before loading.
		valid := make([]core.InternalObject, 0, len(objects))
		for _, obj := range objects {
			if err := validate(obj); err != nil {
				slog.Warn("migrate: validation failed", "id", obj.Id, "err", err)
				stats.Skipped++
				continue
			}
			valid = append(valid, obj)
		}

		if !cfg.DryRun && httpClient != nil && len(valid) > 0 {
			if err := registerBatch(ctx, httpClient, cfg.SyfonURL, valid); err != nil {
				return stats, fmt.Errorf("load batch: %w", err)
			}
			stats.Loaded += len(valid)
			slog.Info("migrate: loaded", "count", len(valid))
		} else {
			stats.Loaded += len(valid) // dry-run: count as "would load"
		}

		// Advance to next page.
		if cursorMode {
			// Cursor-based source: empty nextStart signals end of stream.
			// Never fall back to page mode — cursor and page are mutually
			// exclusive pagination strategies.
			if nextStart == "" {
				break
			}
			cursorStart = nextStart
			// pageNum is irrelevant in cursor mode; leave it at 0.
		} else {
			// Page-based source: a short page means this was the last one.
			if len(records) < batchSize {
				break
			}
			pageNum++
		}
	}

	return stats, nil
}

// validate checks acceptance criteria from issue #20.
func validate(obj core.InternalObject) error {
	if obj.Id == "" {
		return fmt.Errorf("id is empty")
	}
	if len(obj.Checksums) == 0 {
		return fmt.Errorf("no checksums: at least one checksum is required by DRS")
	}
	for _, cs := range obj.Checksums {
		if cs.Type == "" || cs.Checksum == "" {
			return fmt.Errorf("checksum entry has empty type or value")
		}
	}
	return nil
}

// registerBatch POST /index/migrate/bulk — preserves all DRS fields including
// the original source ID, timestamps, version, description, checksums, access
// methods and authz by going through the dedicated migration endpoint, which
// calls database.RegisterObjects directly.
func registerBatch(ctx context.Context, client *http.Client, syfonURL string, objects []core.InternalObject) error {
	recs := make([]migrateWireRecord, 0, len(objects))
	for _, obj := range objects {
		recs = append(recs, migrateWireRecord{
			ID:            obj.Id,
			Name:          obj.Name,
			Size:          obj.Size,
			Version:       obj.Version,
			Description:   obj.Description,
			CreatedTime:   obj.CreatedTime,
			UpdatedTime:   obj.UpdatedTime,
			Checksums:     append([]drs.Checksum(nil), obj.Checksums...),
			AccessMethods: append([]drs.AccessMethod(nil), obj.AccessMethods...),
			Authz:         append([]string(nil), obj.Authorizations...),
		})
	}

	body, err := json.Marshal(migrateWireRequest{Records: recs})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	endpoint := strings.TrimRight(syfonURL, "/") + "/index/migrate/bulk"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("POST %s: unexpected status %d", endpoint, resp.StatusCode)
	}
	return nil
}

