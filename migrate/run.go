package migrate

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

func Run(ctx context.Context, source SourceLister, loader Loader, cfg Config) (Stats, error) {
	if source == nil {
		return Stats{}, fmt.Errorf("source is required")
	}
	if loader == nil && !cfg.DryRun {
		return Stats{}, fmt.Errorf("loader is required")
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	if batchSize > 1024 {
		batchSize = 1024
	}
	sweeps := cfg.Sweeps
	if sweeps <= 0 {
		sweeps = 1
	}

	var stats Stats
	seen := map[string]struct{}{}
	now := time.Now().UTC()

	for sweep := 0; sweep < sweeps; sweep++ {
		start := ""
		sweepNew := 0
		for {
			if cfg.Limit > 0 && stats.CountOfUniqueIDs >= cfg.Limit {
				return stats, nil
			}
			fetchLimit := batchSize
			if cfg.Limit > 0 {
				remaining := cfg.Limit - stats.CountOfUniqueIDs
				if remaining < fetchLimit {
					fetchLimit = remaining
				}
			}
			records, err := source.ListPage(ctx, fetchLimit, start)
			if err != nil {
				return stats, err
			}
			if len(records) == 0 {
				break
			}

			stats.Fetched += len(records)
			filtered := make([]IndexdRecord, 0, len(records))
			maxDID := start
			for _, record := range records {
				if record.DID > maxDID {
					maxDID = record.DID
				}
				if record.DID == "" {
					filtered = append(filtered, record)
					continue
				}
				if _, ok := seen[record.DID]; ok {
					continue
				}
				seen[record.DID] = struct{}{}
				filtered = append(filtered, record)
				sweepNew++
			}
			stats.CountOfUniqueIDs = len(seen)
			if cfg.Limit > 0 && len(filtered) > fetchLimit {
				filtered = filtered[:fetchLimit]
			}

			transformed, transformErrs := TransformBatch(filtered, cfg.DefaultAuthz, now)
			stats.Errors += len(transformErrs)
			for _, err := range transformErrs {
				slog.Warn("migration transform failed", "did", err.DID, "err", err.Err)
			}

			valid := make([]MigrationRecord, 0, len(transformed))
			for _, record := range transformed {
				if err := Validate(record); err != nil {
					stats.Skipped++
					slog.Warn("migration validation skipped record", "id", record.ID, "err", err)
					continue
				}
				valid = append(valid, record)
			}
			stats.Transformed += len(valid)

			if len(valid) > 0 {
				if cfg.DryRun {
					stats.Loaded += len(valid)
				} else {
					if err := loader.LoadBatch(ctx, valid); err != nil {
						return stats, err
					}
					stats.Loaded += len(valid)
				}
			}

			if maxDID == "" || maxDID == start || len(records) < fetchLimit {
				break
			}
			start = maxDID
		}

		if sweepNew == 0 {
			break
		}
	}

	return stats, nil
}

func Import(ctx context.Context, reader DumpReader, loader Loader, batchSize int) (Stats, error) {
	if reader == nil {
		return Stats{}, fmt.Errorf("reader is required")
	}
	if loader == nil {
		return Stats{}, fmt.Errorf("loader is required")
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	var stats Stats
	err := reader.ReadBatches(ctx, batchSize, func(records []MigrationRecord) error {
		valid := make([]MigrationRecord, 0, len(records))
		for _, record := range records {
			stats.Fetched++
			if err := Validate(record); err != nil {
				stats.Skipped++
				slog.Warn("migration import skipped record", "id", record.ID, "err", err)
				continue
			}
			valid = append(valid, record)
		}
		stats.Transformed += len(valid)
		if len(valid) == 0 {
			return nil
		}
		if err := loader.LoadBatch(ctx, valid); err != nil {
			return err
		}
		stats.Loaded += len(valid)
		return nil
	})
	return stats, err
}
