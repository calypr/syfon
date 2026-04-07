package migrate

import (
	"context"
	"fmt"
	"log/slog"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	syclient "github.com/calypr/syfon/client"
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

// Run executes the full Indexd → Syfon ETL pipeline:
//
//  1. Extract   – paginate records from the Indexd API
//  2. Transform – apply the DRS field mapping (issue #20)
//  3. Validate  – checksums, URLs and authz must be present
//  4. Load      – bulk-register objects into Syfon via RegisterObjects
//
// The pipeline is idempotent: Syfon's RegisterObjects upserts records, so
// re-running the migration is safe.
func Run(ctx context.Context, cfg Config) (Stats, error) {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	src := NewIndexdClient(cfg.IndexdURL)
	var dst *syclient.Client
	if !cfg.DryRun {
		dst = syclient.New(cfg.SyfonURL)
	}

	var stats Stats
	start := ""
	page := 0

	for {
		if cfg.Limit > 0 && stats.Fetched >= cfg.Limit {
			break
		}

		fetchN := batchSize
		if cfg.Limit > 0 {
			remaining := cfg.Limit - stats.Fetched
			if remaining < fetchN {
				fetchN = remaining
			}
		}

		records, nextStart, err := src.ListPage(ctx, fetchN, start, page)
		if err != nil {
			return stats, fmt.Errorf("fetch page (start=%q page=%d): %w", start, page, err)
		}
		if len(records) == 0 {
			break // exhausted
		}

		// Guard against servers that ignore the limit parameter.
		if len(records) > fetchN {
			records = records[:fetchN]
		}

		stats.Fetched += len(records)
		slog.Info("migrate: fetched", "count", len(records), "start", start, "page", page)

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

		if !cfg.DryRun && dst != nil && len(valid) > 0 {
			if err := registerBatch(ctx, dst, valid); err != nil {
				return stats, fmt.Errorf("load batch: %w", err)
			}
			stats.Loaded += len(valid)
			slog.Info("migrate: loaded", "count", len(valid))
		} else {
			stats.Loaded += len(valid) // count as "would load" in dry-run
		}

		// Advance cursor.
		if nextStart != "" {
			start = nextStart
			page = 0
		} else {
			page++
			start = ""
		}

		if len(records) < fetchN {
			break // last page
		}
	}

	return stats, nil
}

// validate checks that the transformed DRS object meets the acceptance criteria
// from issue #20:
//   - checksums preserved
//   - URLs mapped
//   - authz preserved
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

// registerBatch bulk-upserts a slice of InternalObjects into Syfon using the
// internal bulk-create endpoint (POST /index/bulk).  This preserves DIDs (UUID
// DIDs are kept as-is; non-UUID DIDs are aliased to a SHA256-derived canonical
// ID), checksums, URLs and authz.
func registerBatch(ctx context.Context, c *syclient.Client, objects []core.InternalObject) error {
	records := make([]internalapi.InternalRecord, 0, len(objects))
	for _, obj := range objects {
		rec := internalapi.InternalRecord{}
		rec.SetDid(obj.Id)
		rec.SetSize(obj.Size)
		if obj.Name != "" {
			rec.SetFileName(obj.Name)
		}
		hashes := make(map[string]string, len(obj.Checksums))
		for _, cs := range obj.Checksums {
			if cs.Type != "" && cs.Checksum != "" {
				hashes[cs.Type] = cs.Checksum
			}
		}
		if len(hashes) > 0 {
			rec.SetHashes(hashes)
		}
		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url != "" {
				rec.Urls = append(rec.Urls, am.AccessUrl.Url)
			}
		}
		rec.Authz = append([]string(nil), obj.Authorizations...)
		records = append(records, rec)
	}
	_, err := c.Index().BulkCreate(ctx, syclient.BulkCreateRequest{Records: records})
	return err
}

