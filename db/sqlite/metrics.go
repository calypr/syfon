package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/drs-server/db/core"
)

func (db *SqliteDB) flushObjectUsageEventsForIDsTx(ctx context.Context, tx *sql.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	placeholders := make([]string, len(ids))
	args := make([]interface{}, 0, len(ids)+1)
	args = append(args, now)
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	inClause := strings.Join(placeholders, ",")
	query := fmt.Sprintf(`
		INSERT INTO object_usage (object_id, upload_count, download_count, last_upload_time, last_download_time, updated_time)
		SELECT e.object_id,
			COALESCE(SUM(CASE WHEN e.event_type = 'upload' THEN 1 ELSE 0 END), 0) AS upload_count,
			COALESCE(SUM(CASE WHEN e.event_type = 'download' THEN 1 ELSE 0 END), 0) AS download_count,
			MAX(CASE WHEN e.event_type = 'upload' THEN e.event_time END) AS last_upload_time,
			MAX(CASE WHEN e.event_type = 'download' THEN e.event_time END) AS last_download_time,
			?
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
		WHERE e.object_id IN (%s)
		GROUP BY e.object_id
		ON CONFLICT(object_id) DO UPDATE SET
			upload_count = object_usage.upload_count + excluded.upload_count,
			download_count = object_usage.download_count + excluded.download_count,
			last_upload_time = CASE
				WHEN excluded.last_upload_time IS NULL THEN object_usage.last_upload_time
				WHEN object_usage.last_upload_time IS NULL THEN excluded.last_upload_time
				WHEN excluded.last_upload_time > object_usage.last_upload_time THEN excluded.last_upload_time
				ELSE object_usage.last_upload_time
			END,
			last_download_time = CASE
				WHEN excluded.last_download_time IS NULL THEN object_usage.last_download_time
				WHEN object_usage.last_download_time IS NULL THEN excluded.last_download_time
				WHEN excluded.last_download_time > object_usage.last_download_time THEN excluded.last_download_time
				ELSE object_usage.last_download_time
			END,
			updated_time = excluded.updated_time
	`, inClause)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return execSQLiteDeleteByIDs(tx, "object_usage_event", ids)
}

func (db *SqliteDB) RecordFileUpload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES (?, 'upload', ?)
	`, objectID, now)
	return err
}

func (db *SqliteDB) RecordFileDownload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES (?, 'download', ?)
	`, objectID, now)
	return err
}

func (db *SqliteDB) GetFileUsage(ctx context.Context, objectID string) (*core.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	var usage core.FileUsage
	var lastUpload sql.NullTime
	var lastDownload sql.NullTime
	err := db.db.QueryRowContext(ctx, `
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
		WHERE o.id = ?
	`, objectID).Scan(
		&usage.ObjectID,
		&usage.Name,
		&usage.Size,
		&usage.UploadCount,
		&usage.DownloadCount,
		&lastUpload,
		&lastDownload,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: file usage not found", core.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	if lastUpload.Valid {
		t := lastUpload.Time
		usage.LastUploadTime = &t
	}
	if lastDownload.Valid {
		t := lastDownload.Time
		usage.LastDownloadTime = &t
	}
	usage.LastAccessTime = latestTime(usage.LastUploadTime, usage.LastDownloadTime)
	return &usage, nil
}

func (db *SqliteDB) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]core.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
	`
	args := []any{}
	if inactiveSince != nil {
		query += ` WHERE u.last_download_time IS NULL OR u.last_download_time < ?`
		args = append(args, inactiveSince.UTC())
	}
	query += ` ORDER BY COALESCE(u.last_download_time, '1970-01-01T00:00:00Z') ASC, o.id ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]core.FileUsage, 0, limit)
	for rows.Next() {
		var usage core.FileUsage
		var lastUpload sql.NullTime
		var lastDownload sql.NullTime
		if err := rows.Scan(
			&usage.ObjectID,
			&usage.Name,
			&usage.Size,
			&usage.UploadCount,
			&usage.DownloadCount,
			&lastUpload,
			&lastDownload,
		); err != nil {
			return nil, err
		}
		if lastUpload.Valid {
			t := lastUpload.Time
			usage.LastUploadTime = &t
		}
		if lastDownload.Valid {
			t := lastDownload.Time
			usage.LastDownloadTime = &t
		}
		usage.LastAccessTime = latestTime(usage.LastUploadTime, usage.LastDownloadTime)
		out = append(out, usage)
	}
	return out, nil
}

func (db *SqliteDB) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (core.FileUsageSummary, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return core.FileUsageSummary{}, err
	}
	summary := core.FileUsageSummary{}
	query := `
		SELECT
			COUNT(o.id) AS total_files,
			COALESCE(SUM(COALESCE(u.upload_count, 0)), 0) AS total_uploads,
			COALESCE(SUM(COALESCE(u.download_count, 0)), 0) AS total_downloads,
			COALESCE(SUM(CASE WHEN u.last_download_time IS NULL OR u.last_download_time < ? THEN 1 ELSE 0 END), 0) AS inactive_files
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
	`
	inactiveCutoff := time.Now().UTC().AddDate(0, 0, -730)
	if inactiveSince != nil {
		inactiveCutoff = inactiveSince.UTC()
	}
	if err := db.db.QueryRowContext(ctx, query, inactiveCutoff).Scan(
		&summary.TotalFiles,
		&summary.TotalUploads,
		&summary.TotalDownloads,
		&summary.InactiveFileCount,
	); err != nil {
		return core.FileUsageSummary{}, err
	}
	return summary, nil
}

func (db *SqliteDB) flushObjectUsageEvents(ctx context.Context) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)
	if err != nil {
		return err
	}
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	rows.Close()

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return err
	}
	return tx.Commit()
}
