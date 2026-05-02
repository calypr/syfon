package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func (db *SqliteDB) flushObjectUsageEventsForIDsTx(ctx context.Context, tx *sql.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	placeholders := make([]string, len(ids))
	capArgs, err := safeSliceCapacity(len(ids), 1)
	if err != nil {
		return err
	}
	args := make([]interface{}, 0, capArgs)
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

func (db *SqliteDB) RecordTransferAttributionEvents(ctx context.Context, events []models.TransferAttributionEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO transfer_attribution_event (
			event_id, access_grant_id, event_type, direction, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url,
			range_start, range_end, bytes_requested, bytes_completed,
			actor_email, actor_subject, auth_mode, client_name, client_version, transfer_session_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, ev := range events {
		if ev.EventID == "" || ev.EventType == "" {
			continue
		}
		if ev.EventType != models.TransferEventAccessIssued {
			continue
		}
		when := ev.EventTime
		if when.IsZero() {
			when = time.Now().UTC()
		}
		ev.AccessGrantID = accessGrantIDFromEvent(ev)
		ev.EventTime = when.UTC()
		ev.Direction = normalizeTransferDirection(ev.Direction)
		result, err := stmt.ExecContext(ctx,
			ev.EventID, ev.AccessGrantID, ev.EventType, ev.Direction, ev.EventTime, ev.RequestID, ev.ObjectID, ev.SHA256, ev.ObjectSize,
			ev.Organization, ev.Project, ev.AccessID, ev.Provider, ev.Bucket, ev.StorageURL,
			nullableInt64(ev.RangeStart), nullableInt64(ev.RangeEnd), ev.BytesRequested, ev.BytesCompleted,
			ev.ActorEmail, ev.ActorSubject, ev.AuthMode, ev.ClientName, ev.ClientVersion, ev.TransferSessionID,
		)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err == nil && rows > 0 {
			if err := sqliteUpsertAccessGrant(ctx, tx, ev); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) RecordProviderTransferEvents(ctx context.Context, events []models.ProviderTransferEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR IGNORE INTO provider_transfer_event (
			provider_event_id, access_grant_id, direction, event_time, request_id, provider_request_id,
			object_id, sha256, object_size, organization, project, access_id, provider, bucket,
			object_key, storage_url, range_start, range_end, bytes_transferred, http_method, http_status,
			requester_principal, source_ip, user_agent, raw_event_ref, actor_email, actor_subject, auth_mode,
			reconciliation_status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i := range events {
		normalized, err := db.reconcileProviderTransferEvent(ctx, tx, events[i])
		if err != nil {
			return err
		}
		events[i] = normalized
		if normalized.ProviderEventID == "" || normalized.Direction == "" || normalized.Provider == "" {
			continue
		}
		when := normalized.EventTime
		if when.IsZero() {
			when = time.Now().UTC()
		}
		if _, err := stmt.ExecContext(ctx,
			normalized.ProviderEventID, normalized.AccessGrantID, normalized.Direction, when.UTC(), normalized.RequestID, normalized.ProviderRequestID,
			normalized.ObjectID, normalized.SHA256, normalized.ObjectSize, normalized.Organization, normalized.Project, normalized.AccessID, normalized.Provider, normalized.Bucket,
			normalized.ObjectKey, normalized.StorageURL, nullableInt64(normalized.RangeStart), nullableInt64(normalized.RangeEnd), normalized.BytesTransferred, normalized.HTTPMethod, normalized.HTTPStatus,
			normalized.RequesterPrincipal, normalized.SourceIP, normalized.UserAgent, normalized.RawEventRef, normalized.ActorEmail, normalized.ActorSubject, normalized.AuthMode,
			normalized.ReconciliationStatus,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) reconcileProviderTransferEvent(ctx context.Context, tx *sql.Tx, ev models.ProviderTransferEvent) (models.ProviderTransferEvent, error) {
	ev.Direction = normalizeProviderDirection(ev.Direction, ev.HTTPMethod)
	ev.Provider = strings.TrimSpace(ev.Provider)
	ev.Bucket = strings.TrimSpace(ev.Bucket)
	ev.ObjectKey = strings.TrimLeft(strings.TrimSpace(ev.ObjectKey), "/")
	ev.StorageURL = strings.TrimSpace(ev.StorageURL)
	ev.ReconciliationStatus = models.ProviderTransferUnmatched
	if ev.AccessGrantID != "" {
		if match, ok, err := sqliteAccessGrantByID(ctx, tx, ev.AccessGrantID); err != nil {
			return ev, err
		} else if ok {
			mergeAccessGrantIntoProviderEvent(&ev, match)
			ev.ReconciliationStatus = models.ProviderTransferMatched
			return ev, nil
		}
	}
	matches, err := sqliteAccessGrantCandidates(ctx, tx, ev)
	if err != nil {
		return ev, err
	}
	switch len(matches) {
	case 0:
		return ev, nil
	case 1:
		mergeAccessGrantIntoProviderEvent(&ev, matches[0])
		ev.ReconciliationStatus = models.ProviderTransferMatched
	default:
		ev.ReconciliationStatus = models.ProviderTransferAmbiguous
	}
	return ev, nil
}

func (db *SqliteDB) GetTransferAttributionSummary(ctx context.Context, filter models.TransferAttributionFilter) (models.TransferAttributionSummary, error) {
	where, args := transferAttributionWhere(filter)
	var out models.TransferAttributionSummary
	err := db.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN event_type = 'access_issued' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(bytes_requested), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_requested ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_requested ELSE 0 END), 0)
		FROM transfer_attribution_event`+where, args...).Scan(
		&out.EventCount,
		&out.AccessIssuedCount,
		&out.DownloadEventCount,
		&out.UploadEventCount,
		&out.BytesRequested,
		&out.BytesDownloaded,
		&out.BytesUploaded,
	)
	return out, err
}

func (db *SqliteDB) GetTransferAttributionBreakdown(ctx context.Context, filter models.TransferAttributionFilter, groupBy string) ([]models.TransferAttributionBreakdown, error) {
	keyExpr, selectExpr := transferAttributionGroupExpr(groupBy)
	where, args := transferAttributionWhere(filter)
	query := fmt.Sprintf(`
		SELECT %s,
			COUNT(*),
			COALESCE(SUM(bytes_requested), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_requested ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_requested ELSE 0 END), 0),
			MAX(event_time)
		FROM transfer_attribution_event%s
		GROUP BY %s
		ORDER BY MAX(event_time) DESC, key ASC
		LIMIT 1000
	`, selectExpr, where, keyExpr)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTransferAttributionBreakdown(rows)
}

func (db *SqliteDB) GetTransferAttributionSummaryByResources(ctx context.Context, filter models.TransferAttributionFilter, resources []string) (models.TransferAttributionSummary, error) {
	where, args := transferAttributionWhereByResources(filter, resources)
	var out models.TransferAttributionSummary
	err := db.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN event_type = 'access_issued' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(bytes_requested), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_requested ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_requested ELSE 0 END), 0)
		FROM transfer_attribution_event`+where, args...).Scan(
		&out.EventCount,
		&out.AccessIssuedCount,
		&out.DownloadEventCount,
		&out.UploadEventCount,
		&out.BytesRequested,
		&out.BytesDownloaded,
		&out.BytesUploaded,
	)
	return out, err
}

func (db *SqliteDB) GetTransferAttributionBreakdownByResources(ctx context.Context, filter models.TransferAttributionFilter, groupBy string, resources []string) ([]models.TransferAttributionBreakdown, error) {
	keyExpr, selectExpr := transferAttributionGroupExpr(groupBy)
	where, args := transferAttributionWhereByResources(filter, resources)
	query := fmt.Sprintf(`
		SELECT %s,
			COUNT(*),
			COALESCE(SUM(bytes_requested), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_requested ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_requested ELSE 0 END), 0),
			MAX(event_time)
		FROM transfer_attribution_event%s
		GROUP BY %s
		ORDER BY MAX(event_time) DESC, key ASC
		LIMIT 1000
	`, selectExpr, where, keyExpr)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTransferAttributionBreakdown(rows)
}

func (db *SqliteDB) GetFileUsage(ctx context.Context, objectID string) (*models.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	var usage models.FileUsage
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
		return nil, fmt.Errorf("%w: file usage not found", common.ErrNotFound)
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

func (db *SqliteDB) ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]models.FileUsage, error) {
	if len(ids) == 0 {
		return []models.FileUsage{}, nil
	}
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}

	out := make([]models.FileUsage, 0, len(ids))
	for start := 0; start < len(ids); start += sqliteMaxParams {
		end := start + sqliteMaxParams
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]
		args := make([]any, 0, len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}
		rows, err := db.db.QueryContext(ctx, `
			SELECT o.id, o.name, o.size,
				COALESCE(u.upload_count, 0),
				COALESCE(u.download_count, 0),
				u.last_upload_time,
				u.last_download_time
			FROM drs_object o
			LEFT JOIN object_usage u ON u.object_id = o.id
			WHERE o.id IN (`+makePlaceholders(len(chunk))+`)
			ORDER BY o.id
		`, args...)
		if err != nil {
			return nil, err
		}
		usages, err := scanFileUsageRows(rows, len(chunk))
		rows.Close()
		if err != nil {
			return nil, err
		}
		out = append(out, usages...)
	}
	return out, nil
}

func (db *SqliteDB) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error) {
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

	return scanFileUsageRows(rows, limit)
}

func (db *SqliteDB) ListFileUsagePageByScope(ctx context.Context, organization, project string, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error) {
	resource, err := sycommon.ResourcePath(strings.TrimSpace(organization), strings.TrimSpace(project))
	if err != nil {
		return nil, err
	}
	return db.listScopedFileUsagePage(ctx, []string{resource}, false, limit, offset, inactiveSince)
}

func (db *SqliteDB) ListFileUsagePageByResources(ctx context.Context, resources []string, includeUnscoped bool, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error) {
	return db.listScopedFileUsagePage(ctx, resources, includeUnscoped, limit, offset, inactiveSince)
}

func (db *SqliteDB) GetFileUsageSummaryByScope(ctx context.Context, organization, project string, inactiveSince *time.Time) (models.FileUsageSummary, error) {
	resource, err := sycommon.ResourcePath(strings.TrimSpace(organization), strings.TrimSpace(project))
	if err != nil {
		return models.FileUsageSummary{}, err
	}
	return db.getScopedFileUsageSummary(ctx, []string{resource}, false, inactiveSince)
}

func (db *SqliteDB) GetFileUsageSummaryByResources(ctx context.Context, resources []string, includeUnscoped bool, inactiveSince *time.Time) (models.FileUsageSummary, error) {
	return db.getScopedFileUsageSummary(ctx, resources, includeUnscoped, inactiveSince)
}

func scanFileUsageRows(rows *sql.Rows, capacity int) ([]models.FileUsage, error) {
	out := make([]models.FileUsage, 0, capacity)
	for rows.Next() {
		var usage models.FileUsage
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *SqliteDB) listScopedFileUsagePage(ctx context.Context, resources []string, includeUnscoped bool, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return []models.FileUsage{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	resources = sycommon.NormalizeAccessResources(resources)
	if len(resources) == 0 && !includeUnscoped {
		return []models.FileUsage{}, nil
	}

	query, args := sqliteScopedFileUsageQuery(resources, includeUnscoped, inactiveSince, false)
	query += ` ORDER BY COALESCE(u.last_download_time, '1970-01-01T00:00:00Z') ASC, o.id ASC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanFileUsageRows(rows, limit)
}

func (db *SqliteDB) getScopedFileUsageSummary(ctx context.Context, resources []string, includeUnscoped bool, inactiveSince *time.Time) (models.FileUsageSummary, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return models.FileUsageSummary{}, err
	}
	resources = sycommon.NormalizeAccessResources(resources)
	if len(resources) == 0 && !includeUnscoped {
		return models.FileUsageSummary{}, nil
	}

	query, args := sqliteScopedFileUsageQuery(resources, includeUnscoped, inactiveSince, true)
	var summary models.FileUsageSummary
	if err := db.db.QueryRowContext(ctx, query, args...).Scan(
		&summary.TotalFiles,
		&summary.TotalUploads,
		&summary.TotalDownloads,
		&summary.InactiveFileCount,
	); err != nil {
		return models.FileUsageSummary{}, err
	}
	return summary, nil
}

func sqliteScopedFileUsageQuery(resources []string, includeUnscoped bool, inactiveSince *time.Time, summary bool) (string, []any) {
	args := make([]any, 0, len(resources)+2)
	parts := make([]string, 0, 2)
	if len(resources) > 0 {
		placeholders := make([]string, 0, len(resources))
		for _, resource := range resources {
			args = append(args, resource)
			placeholders = append(placeholders, "?")
		}
		parts = append(parts, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id AND ca.resource IN (`+strings.Join(placeholders, ",")+`)
		)`)
	}
	if includeUnscoped {
		parts = append(parts, `NOT EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id
		)`)
	}

	var selectClause string
	if summary {
		if inactiveSince != nil {
			args = append(args, inactiveSince.UTC())
			selectClause = `
		SELECT
			COUNT(o.id) AS total_files,
			COALESCE(SUM(COALESCE(u.upload_count, 0)), 0) AS total_uploads,
			COALESCE(SUM(COALESCE(u.download_count, 0)), 0) AS total_downloads,
			COALESCE(SUM(CASE WHEN u.last_download_time IS NULL OR u.last_download_time < ? THEN 1 ELSE 0 END), 0) AS inactive_files`
		} else {
			selectClause = `
		SELECT
			COUNT(o.id) AS total_files,
			COALESCE(SUM(COALESCE(u.upload_count, 0)), 0) AS total_uploads,
			COALESCE(SUM(COALESCE(u.download_count, 0)), 0) AS total_downloads,
			0 AS inactive_files`
		}
	} else {
		selectClause = `
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time`
	}

	query := selectClause + `
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
		WHERE ((` + strings.Join(parts, " OR ") + `))`
	if !summary && inactiveSince != nil {
		args = append(args, inactiveSince.UTC())
		query += ` AND (u.last_download_time IS NULL OR u.last_download_time < ?)`
	}
	return query, args
}

func (db *SqliteDB) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (models.FileUsageSummary, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return models.FileUsageSummary{}, err
	}
	summary := models.FileUsageSummary{}
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
		return models.FileUsageSummary{}, err
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

func nullableInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

func nullableTime(v *time.Time) any {
	if v == nil {
		return nil
	}
	return v.UTC()
}

func (db *SqliteDB) backfillAccessGrants(ctx context.Context) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, access_grant_id, event_type, direction, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url, range_start, range_end,
			bytes_requested, bytes_completed, actor_email, actor_subject, auth_mode, client_name, client_version,
			transfer_session_id
		FROM transfer_attribution_event
		WHERE event_type = 'access_issued'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	events := make([]models.TransferAttributionEvent, 0)
	for rows.Next() {
		var ev models.TransferAttributionEvent
		if err := rows.Scan(
			&ev.EventID, &ev.AccessGrantID, &ev.EventType, &ev.Direction, &ev.EventTime, &ev.RequestID, &ev.ObjectID, &ev.SHA256, &ev.ObjectSize,
			&ev.Organization, &ev.Project, &ev.AccessID, &ev.Provider, &ev.Bucket, &ev.StorageURL, &ev.RangeStart, &ev.RangeEnd,
			&ev.BytesRequested, &ev.BytesCompleted, &ev.ActorEmail, &ev.ActorSubject, &ev.AuthMode, &ev.ClientName, &ev.ClientVersion,
			&ev.TransferSessionID,
		); err != nil {
			return err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	grants := make(map[string]models.AccessGrant)
	for _, ev := range events {
		ev.AccessGrantID = accessGrantIDFromEvent(ev)
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_attribution_event SET access_grant_id = ? WHERE event_id = ?`, ev.AccessGrantID, ev.EventID); err != nil {
			return err
		}
		grant := grants[ev.AccessGrantID]
		when := ev.EventTime.UTC()
		if grant.AccessGrantID == "" {
			grant = models.AccessGrant{
				AccessGrantID: ev.AccessGrantID,
				FirstIssuedAt: when,
				LastIssuedAt:  when,
				ObjectID:      ev.ObjectID,
				SHA256:        ev.SHA256,
				ObjectSize:    ev.ObjectSize,
				Organization:  ev.Organization,
				Project:       ev.Project,
				AccessID:      ev.AccessID,
				Provider:      ev.Provider,
				Bucket:        ev.Bucket,
				StorageURL:    ev.StorageURL,
				ActorEmail:    ev.ActorEmail,
				ActorSubject:  ev.ActorSubject,
				AuthMode:      ev.AuthMode,
			}
		}
		if when.Before(grant.FirstIssuedAt) {
			grant.FirstIssuedAt = when
		}
		if when.After(grant.LastIssuedAt) {
			grant.LastIssuedAt = when
		}
		grant.IssueCount++
		if grant.ActorEmail == "" {
			grant.ActorEmail = ev.ActorEmail
		}
		if grant.ActorSubject == "" {
			grant.ActorSubject = ev.ActorSubject
		}
		if grant.AuthMode == "" {
			grant.AuthMode = ev.AuthMode
		}
		grants[ev.AccessGrantID] = grant
	}
	for _, grant := range grants {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO access_grant (
				access_grant_id, first_issued_at, last_issued_at, issue_count,
				object_id, sha256, object_size, organization, project, access_id,
				provider, bucket, storage_url, actor_email, actor_subject, auth_mode
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, grant.AccessGrantID, grant.FirstIssuedAt, grant.LastIssuedAt, grant.IssueCount,
			grant.ObjectID, grant.SHA256, grant.ObjectSize, grant.Organization, grant.Project, grant.AccessID,
			grant.Provider, grant.Bucket, grant.StorageURL, grant.ActorEmail, grant.ActorSubject, grant.AuthMode); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func accessGrantIDFromEvent(ev models.TransferAttributionEvent) string {
	parts := []string{
		ev.ObjectID,
		ev.SHA256,
		ev.Organization,
		ev.Project,
		ev.AccessID,
		ev.Provider,
		ev.Bucket,
		ev.StorageURL,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])
}

func sqliteUpsertAccessGrant(ctx context.Context, tx *sql.Tx, ev models.TransferAttributionEvent) error {
	if ev.AccessGrantID == "" {
		return nil
	}
	when := ev.EventTime.UTC()
	_, err := tx.ExecContext(ctx, `
		INSERT INTO access_grant (
			access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(access_grant_id) DO UPDATE SET
			first_issued_at = CASE
				WHEN excluded.first_issued_at < access_grant.first_issued_at THEN excluded.first_issued_at
				ELSE access_grant.first_issued_at
			END,
			last_issued_at = CASE
				WHEN excluded.last_issued_at > access_grant.last_issued_at THEN excluded.last_issued_at
				ELSE access_grant.last_issued_at
			END,
			issue_count = access_grant.issue_count + 1,
			object_id = COALESCE(NULLIF(access_grant.object_id, ''), excluded.object_id),
			sha256 = COALESCE(NULLIF(access_grant.sha256, ''), excluded.sha256),
			object_size = CASE WHEN access_grant.object_size = 0 THEN excluded.object_size ELSE access_grant.object_size END,
			organization = COALESCE(NULLIF(access_grant.organization, ''), excluded.organization),
			project = COALESCE(NULLIF(access_grant.project, ''), excluded.project),
			access_id = COALESCE(NULLIF(access_grant.access_id, ''), excluded.access_id),
			provider = COALESCE(NULLIF(access_grant.provider, ''), excluded.provider),
			bucket = COALESCE(NULLIF(access_grant.bucket, ''), excluded.bucket),
			storage_url = COALESCE(NULLIF(access_grant.storage_url, ''), excluded.storage_url),
			actor_email = COALESCE(NULLIF(access_grant.actor_email, ''), excluded.actor_email),
			actor_subject = COALESCE(NULLIF(access_grant.actor_subject, ''), excluded.actor_subject),
			auth_mode = COALESCE(NULLIF(access_grant.auth_mode, ''), excluded.auth_mode)
	`, ev.AccessGrantID, when, when, ev.ObjectID, ev.SHA256, ev.ObjectSize,
		ev.Organization, ev.Project, ev.AccessID, ev.Provider, ev.Bucket, ev.StorageURL,
		ev.ActorEmail, ev.ActorSubject, ev.AuthMode)
	return err
}

func sqliteAccessGrantByID(ctx context.Context, tx *sql.Tx, grantID string) (models.AccessGrant, bool, error) {
	var grant models.AccessGrant
	err := tx.QueryRowContext(ctx, `
		SELECT access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		FROM access_grant
		WHERE access_grant_id = ?
	`, grantID).Scan(
		&grant.AccessGrantID, &grant.FirstIssuedAt, &grant.LastIssuedAt, &grant.IssueCount,
		&grant.ObjectID, &grant.SHA256, &grant.ObjectSize, &grant.Organization, &grant.Project, &grant.AccessID,
		&grant.Provider, &grant.Bucket, &grant.StorageURL, &grant.ActorEmail, &grant.ActorSubject, &grant.AuthMode,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return models.AccessGrant{}, false, nil
	}
	return grant, err == nil, err
}

func sqliteAccessGrantCandidates(ctx context.Context, tx *sql.Tx, ev models.ProviderTransferEvent) ([]models.AccessGrant, error) {
	query := `
		SELECT access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		FROM access_grant
		WHERE provider = ?
			AND bucket = ?
			AND last_issued_at <= ?
			AND last_issued_at >= ?
	`
	args := []any{ev.Provider, ev.Bucket, ev.EventTime.UTC().Add(15 * time.Minute), ev.EventTime.UTC().Add(-24 * time.Hour)}
	if ev.StorageURL != "" {
		query += " AND storage_url = ?"
		args = append(args, ev.StorageURL)
	} else if ev.ObjectKey != "" {
		query += " AND (storage_url = ? OR storage_url LIKE ?)"
		args = append(args, providerStorageURL(ev.Provider, ev.Bucket, ev.ObjectKey), "%/"+ev.ObjectKey)
	}
	if ev.Direction == models.ProviderTransferDirectionDownload {
		query += " AND object_size >= 0"
	}
	query += " ORDER BY last_issued_at DESC LIMIT 2"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.AccessGrant
	for rows.Next() {
		var match models.AccessGrant
		if err := rows.Scan(
			&match.AccessGrantID, &match.FirstIssuedAt, &match.LastIssuedAt, &match.IssueCount,
			&match.ObjectID, &match.SHA256, &match.ObjectSize, &match.Organization, &match.Project, &match.AccessID,
			&match.Provider, &match.Bucket, &match.StorageURL, &match.ActorEmail, &match.ActorSubject, &match.AuthMode,
		); err != nil {
			return nil, err
		}
		out = append(out, match)
	}
	return out, rows.Err()
}

func mergeAccessGrantIntoProviderEvent(ev *models.ProviderTransferEvent, grant models.AccessGrant) {
	if ev.AccessGrantID == "" {
		ev.AccessGrantID = grant.AccessGrantID
	}
	if ev.ObjectID == "" {
		ev.ObjectID = grant.ObjectID
	}
	if ev.SHA256 == "" {
		ev.SHA256 = grant.SHA256
	}
	if ev.ObjectSize == 0 {
		ev.ObjectSize = grant.ObjectSize
	}
	if ev.Organization == "" {
		ev.Organization = grant.Organization
	}
	if ev.Project == "" {
		ev.Project = grant.Project
	}
	if ev.AccessID == "" {
		ev.AccessID = grant.AccessID
	}
	if ev.StorageURL == "" {
		ev.StorageURL = grant.StorageURL
	}
	hasActor := ev.ActorEmail != "" || ev.ActorSubject != ""
	if !hasActor {
		ev.ActorEmail = grant.ActorEmail
	}
	if !hasActor {
		ev.ActorSubject = grant.ActorSubject
	}
	if ev.AuthMode == "" {
		ev.AuthMode = grant.AuthMode
	}
}

func normalizeProviderDirection(direction, method string) string {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case models.ProviderTransferDirectionDownload, "get", "read":
		return models.ProviderTransferDirectionDownload
	case models.ProviderTransferDirectionUpload, "put", "write":
		return models.ProviderTransferDirectionUpload
	}
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "GET":
		return models.ProviderTransferDirectionDownload
	case "PUT", "POST":
		return models.ProviderTransferDirectionUpload
	default:
		return strings.ToLower(strings.TrimSpace(direction))
	}
}

func providerStorageURL(provider, bucket, key string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "gcs", "gs":
		return "gs://" + bucket + "/" + strings.TrimLeft(key, "/")
	case "azure", "az":
		return "az://" + bucket + "/" + strings.TrimLeft(key, "/")
	default:
		return "s3://" + bucket + "/" + strings.TrimLeft(key, "/")
	}
}

func transferAttributionWhere(filter models.TransferAttributionFilter) (string, []any) {
	parts := make([]string, 0)
	args := make([]any, 0)
	add := func(clause string, value any) {
		parts = append(parts, clause)
		args = append(args, value)
	}
	if strings.TrimSpace(filter.Organization) != "" {
		add("organization = ?", strings.TrimSpace(filter.Organization))
	}
	if strings.TrimSpace(filter.Project) != "" {
		add("project = ?", strings.TrimSpace(filter.Project))
	}
	if strings.TrimSpace(filter.EventType) != "" && strings.TrimSpace(filter.EventType) != "all" {
		add("event_type = ?", strings.TrimSpace(filter.EventType))
	}
	direction := strings.TrimSpace(filter.Direction)
	if direction == "" {
		switch strings.TrimSpace(filter.EventType) {
		case models.ProviderTransferDirectionDownload:
			direction = models.ProviderTransferDirectionDownload
		case models.ProviderTransferDirectionUpload:
			direction = models.ProviderTransferDirectionUpload
		}
	}
	if direction != "" && direction != "all" {
		add("direction = ?", direction)
	}
	if filter.From != nil {
		add("event_time >= ?", filter.From.UTC())
	}
	if filter.To != nil {
		add("event_time <= ?", filter.To.UTC())
	}
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider = ?", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket = ?", strings.TrimSpace(filter.Bucket))
	}
	if strings.TrimSpace(filter.SHA256) != "" {
		add("sha256 = ?", strings.TrimSpace(filter.SHA256))
	}
	if strings.TrimSpace(filter.User) != "" {
		user := strings.TrimSpace(filter.User)
		parts = append(parts, "(actor_email = ? OR actor_subject = ?)")
		args = append(args, user, user)
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func transferAttributionWhereByResources(filter models.TransferAttributionFilter, resources []string) (string, []any) {
	where, args := transferAttributionWhere(filter)
	clause, clauseArgs := sqliteTransferResourceClause(resources)
	if clause == "" {
		if where == "" {
			return " WHERE 1 = 0", args
		}
		return where + " AND 1 = 0", args
	}
	if where == "" {
		return " WHERE " + clause, append(args, clauseArgs...)
	}
	return where + " AND (" + clause + ")", append(args, clauseArgs...)
}

func sqliteTransferResourceClause(resources []string) (string, []any) {
	resources = sycommon.NormalizeAccessResources(resources)
	if len(resources) == 0 {
		return "", nil
	}

	orgOnly := make([]string, 0)
	orgSeen := make(map[string]struct{})
	projectClauses := make([]string, 0)
	args := make([]any, 0, len(resources)*2)
	for _, resource := range resources {
		org, project, ok := sycommon.ResourceScope(resource)
		if !ok {
			continue
		}
		if project == "" {
			if _, exists := orgSeen[org]; exists {
				continue
			}
			orgSeen[org] = struct{}{}
			orgOnly = append(orgOnly, org)
			continue
		}
		projectClauses = append(projectClauses, "(organization = ? AND project = ?)")
		args = append(args, org, project)
	}

	clauses := make([]string, 0, 2)
	if len(orgOnly) > 0 {
		placeholders := make([]string, 0, len(orgOnly))
		for _, org := range orgOnly {
			placeholders = append(placeholders, "?")
			args = append(args, org)
		}
		clauses = append(clauses, "organization IN ("+strings.Join(placeholders, ",")+")")
	}
	if len(projectClauses) > 0 {
		clauses = append(clauses, strings.Join(projectClauses, " OR "))
	}
	return strings.Join(clauses, " OR "), args
}

func providerTransferWhere(filter models.TransferAttributionFilter) (string, []any) {
	parts := make([]string, 0)
	args := make([]any, 0)
	add := func(clause string, value any) {
		parts = append(parts, clause)
		args = append(args, value)
	}
	status := strings.TrimSpace(filter.ReconciliationStatus)
	if status == "" {
		status = models.ProviderTransferMatched
	}
	if status != "all" {
		add("reconciliation_status = ?", status)
	}
	if strings.TrimSpace(filter.Organization) != "" {
		add("organization = ?", strings.TrimSpace(filter.Organization))
	}
	if strings.TrimSpace(filter.Project) != "" {
		add("project = ?", strings.TrimSpace(filter.Project))
	}
	direction := strings.TrimSpace(filter.Direction)
	if direction == "" {
		switch strings.TrimSpace(filter.EventType) {
		case models.ProviderTransferDirectionDownload:
			direction = models.ProviderTransferDirectionDownload
		case models.ProviderTransferDirectionUpload:
			direction = models.ProviderTransferDirectionUpload
		}
	}
	if direction != "" && direction != "all" {
		add("direction = ?", direction)
	}
	if filter.From != nil {
		add("event_time >= ?", filter.From.UTC())
	}
	if filter.To != nil {
		add("event_time <= ?", filter.To.UTC())
	}
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider = ?", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket = ?", strings.TrimSpace(filter.Bucket))
	}
	if strings.TrimSpace(filter.SHA256) != "" {
		add("sha256 = ?", strings.TrimSpace(filter.SHA256))
	}
	if strings.TrimSpace(filter.User) != "" {
		user := strings.TrimSpace(filter.User)
		parts = append(parts, "(actor_email = ? OR actor_subject = ?)")
		args = append(args, user, user)
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func providerTransferGroupExpr(groupBy string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(groupBy)) {
	case "user":
		return "COALESCE(NULLIF(actor_email, ''), actor_subject)", "COALESCE(NULLIF(actor_email, ''), actor_subject) AS key, '' AS organization, '' AS project, '' AS provider, '' AS bucket, '' AS sha256, actor_email, actor_subject"
	case "provider":
		return "provider, bucket", "provider || ':' || bucket AS key, '' AS organization, '' AS project, provider, bucket, '' AS sha256, '' AS actor_email, '' AS actor_subject"
	case "object":
		return "sha256", "sha256 AS key, '' AS organization, '' AS project, '' AS provider, '' AS bucket, sha256, '' AS actor_email, '' AS actor_subject"
	default:
		return "organization, project", "organization || '/' || project AS key, organization, project, '' AS provider, '' AS bucket, '' AS sha256, '' AS actor_email, '' AS actor_subject"
	}
}

func transferAttributionGroupExpr(groupBy string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(groupBy)) {
	case "user":
		return "COALESCE(NULLIF(actor_email, ''), actor_subject)", "COALESCE(NULLIF(actor_email, ''), actor_subject) AS key, '' AS organization, '' AS project, '' AS provider, '' AS bucket, '' AS sha256, actor_email, actor_subject"
	case "provider":
		return "provider, bucket", "provider || ':' || bucket AS key, '' AS organization, '' AS project, provider, bucket, '' AS sha256, '' AS actor_email, '' AS actor_subject"
	case "object":
		return "sha256", "sha256 AS key, '' AS organization, '' AS project, '' AS provider, '' AS bucket, sha256, '' AS actor_email, '' AS actor_subject"
	default:
		return "organization, project", "organization || '/' || project AS key, organization, project, '' AS provider, '' AS bucket, '' AS sha256, '' AS actor_email, '' AS actor_subject"
	}
}

func normalizeTransferDirection(direction string) string {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case models.ProviderTransferDirectionUpload:
		return models.ProviderTransferDirectionUpload
	default:
		return models.ProviderTransferDirectionDownload
	}
}

type transferRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanTransferAttributionBreakdown(rows transferRows) ([]models.TransferAttributionBreakdown, error) {
	out := make([]models.TransferAttributionBreakdown, 0)
	for rows.Next() {
		var item models.TransferAttributionBreakdown
		var last any
		if err := rows.Scan(
			&item.Key,
			&item.Organization,
			&item.Project,
			&item.Provider,
			&item.Bucket,
			&item.SHA256,
			&item.ActorEmail,
			&item.ActorSubject,
			&item.EventCount,
			&item.BytesRequested,
			&item.BytesDownloaded,
			&item.BytesUploaded,
			&last,
		); err != nil {
			return nil, err
		}
		if t, ok := parseSQLiteTransferTime(last); ok {
			item.LastTransferTime = &t
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func parseSQLiteTransferTime(value any) (time.Time, bool) {
	switch v := value.(type) {
	case time.Time:
		if v.IsZero() {
			return time.Time{}, false
		}
		return v.UTC(), true
	case string:
		return parseSQLiteTransferTimeString(v)
	case []byte:
		return parseSQLiteTransferTimeString(string(v))
	default:
		return time.Time{}, false
	}
}

func parseSQLiteTransferTimeString(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
