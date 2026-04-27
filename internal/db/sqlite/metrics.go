package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

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
			event_id, access_grant_id, event_type, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url,
			range_start, range_end, bytes_requested, bytes_completed,
			actor_email, actor_subject, auth_mode, client_name, client_version, transfer_session_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
		if ev.AccessGrantID == "" {
			ev.AccessGrantID = ev.EventID
		}
		if _, err := stmt.ExecContext(ctx,
			ev.EventID, ev.AccessGrantID, ev.EventType, when.UTC(), ev.RequestID, ev.ObjectID, ev.SHA256, ev.ObjectSize,
			ev.Organization, ev.Project, ev.AccessID, ev.Provider, ev.Bucket, ev.StorageURL,
			nullableInt64(ev.RangeStart), nullableInt64(ev.RangeEnd), ev.BytesRequested, ev.BytesCompleted,
			ev.ActorEmail, ev.ActorSubject, ev.AuthMode, ev.ClientName, ev.ClientVersion, ev.TransferSessionID,
		); err != nil {
			return err
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

func (db *SqliteDB) RecordProviderTransferSyncRuns(ctx context.Context, runs []models.ProviderTransferSyncRun) error {
	if len(runs) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT OR REPLACE INTO provider_transfer_sync_run (
			sync_id, provider, bucket, organization, project, from_time, to_time, status,
			requested_at, started_at, completed_at, imported_events, matched_events,
			ambiguous_events, unmatched_events, error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, run := range runs {
		if run.SyncID == "" {
			continue
		}
		if _, err := stmt.ExecContext(ctx,
			run.SyncID, run.Provider, run.Bucket, run.Organization, run.Project, run.From.UTC(), run.To.UTC(), run.Status,
			run.RequestedAt.UTC(), nullableTime(run.StartedAt), nullableTime(run.CompletedAt), run.ImportedEvents, run.MatchedEvents,
			run.AmbiguousEvents, run.UnmatchedEvents, run.ErrorMessage,
		); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) ListProviderTransferSyncRuns(ctx context.Context, filter models.TransferAttributionFilter, limit int) ([]models.ProviderTransferSyncRun, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	where, args := providerSyncWhere(filter)
	args = append(args, limit)
	rows, err := db.db.QueryContext(ctx, `
		SELECT sync_id, provider, bucket, organization, project, from_time, to_time, status,
			requested_at, started_at, completed_at, imported_events, matched_events,
			ambiguous_events, unmatched_events, error_message
		FROM provider_transfer_sync_run`+where+`
		ORDER BY requested_at DESC, sync_id ASC
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProviderSyncRuns(rows)
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
	where, args := providerTransferWhere(filter)
	var out models.TransferAttributionSummary
	err := db.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			0,
			COALESCE(SUM(CASE WHEN direction = 'download' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(bytes_transferred), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_transferred ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_transferred ELSE 0 END), 0)
		FROM provider_transfer_event`+where, args...).Scan(
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
	keyExpr, selectExpr := providerTransferGroupExpr(groupBy)
	where, args := providerTransferWhere(filter)
	query := fmt.Sprintf(`
		SELECT %s,
			COUNT(*),
			COALESCE(SUM(bytes_transferred), 0),
			COALESCE(SUM(CASE WHEN direction = 'download' THEN bytes_transferred ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'upload' THEN bytes_transferred ELSE 0 END), 0),
			MAX(event_time)
		FROM provider_transfer_event%s
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

	out := make([]models.FileUsage, 0, limit)
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
	return out, nil
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

func providerSyncWhere(filter models.TransferAttributionFilter) (string, []any) {
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
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider = ?", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket = ?", strings.TrimSpace(filter.Bucket))
	}
	if filter.From != nil {
		add("to_time >= ?", filter.From.UTC())
	}
	if filter.To != nil {
		add("from_time <= ?", filter.To.UTC())
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func scanProviderSyncRuns(rows transferRows) ([]models.ProviderTransferSyncRun, error) {
	out := make([]models.ProviderTransferSyncRun, 0)
	for rows.Next() {
		var run models.ProviderTransferSyncRun
		var started, completed sql.NullTime
		if err := rows.Scan(
			&run.SyncID,
			&run.Provider,
			&run.Bucket,
			&run.Organization,
			&run.Project,
			&run.From,
			&run.To,
			&run.Status,
			&run.RequestedAt,
			&started,
			&completed,
			&run.ImportedEvents,
			&run.MatchedEvents,
			&run.AmbiguousEvents,
			&run.UnmatchedEvents,
			&run.ErrorMessage,
		); err != nil {
			return nil, err
		}
		run.From = run.From.UTC()
		run.To = run.To.UTC()
		run.RequestedAt = run.RequestedAt.UTC()
		if started.Valid {
			t := started.Time.UTC()
			run.StartedAt = &t
		}
		if completed.Valid {
			t := completed.Time.UTC()
			run.CompletedAt = &t
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func sqliteAccessGrantByID(ctx context.Context, tx *sql.Tx, grantID string) (models.TransferAttributionEvent, bool, error) {
	var ev models.TransferAttributionEvent
	err := tx.QueryRowContext(ctx, `
		SELECT event_id, access_grant_id, event_type, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url, range_start, range_end,
			bytes_requested, bytes_completed, actor_email, actor_subject, auth_mode, client_name, client_version,
			transfer_session_id
		FROM transfer_attribution_event
		WHERE access_grant_id = ? OR event_id = ?
		ORDER BY event_time DESC
		LIMIT 1
	`, grantID, grantID).Scan(
		&ev.EventID, &ev.AccessGrantID, &ev.EventType, &ev.EventTime, &ev.RequestID, &ev.ObjectID, &ev.SHA256, &ev.ObjectSize,
		&ev.Organization, &ev.Project, &ev.AccessID, &ev.Provider, &ev.Bucket, &ev.StorageURL, &ev.RangeStart, &ev.RangeEnd,
		&ev.BytesRequested, &ev.BytesCompleted, &ev.ActorEmail, &ev.ActorSubject, &ev.AuthMode, &ev.ClientName, &ev.ClientVersion,
		&ev.TransferSessionID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return models.TransferAttributionEvent{}, false, nil
	}
	return ev, err == nil, err
}

func sqliteAccessGrantCandidates(ctx context.Context, tx *sql.Tx, ev models.ProviderTransferEvent) ([]models.TransferAttributionEvent, error) {
	query := `
		SELECT event_id, access_grant_id, event_type, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url, range_start, range_end,
			bytes_requested, bytes_completed, actor_email, actor_subject, auth_mode, client_name, client_version,
			transfer_session_id
		FROM transfer_attribution_event
		WHERE event_type = 'access_issued'
			AND provider = ?
			AND bucket = ?
			AND event_time <= ?
			AND event_time >= ?
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
		query += " AND bytes_requested >= 0"
	}
	query += " ORDER BY event_time DESC LIMIT 2"
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []models.TransferAttributionEvent
	for rows.Next() {
		var match models.TransferAttributionEvent
		if err := rows.Scan(
			&match.EventID, &match.AccessGrantID, &match.EventType, &match.EventTime, &match.RequestID, &match.ObjectID, &match.SHA256, &match.ObjectSize,
			&match.Organization, &match.Project, &match.AccessID, &match.Provider, &match.Bucket, &match.StorageURL, &match.RangeStart, &match.RangeEnd,
			&match.BytesRequested, &match.BytesCompleted, &match.ActorEmail, &match.ActorSubject, &match.AuthMode, &match.ClientName, &match.ClientVersion,
			&match.TransferSessionID,
		); err != nil {
			return nil, err
		}
		out = append(out, match)
	}
	return out, rows.Err()
}

func mergeAccessGrantIntoProviderEvent(ev *models.ProviderTransferEvent, grant models.TransferAttributionEvent) {
	if ev.AccessGrantID == "" {
		ev.AccessGrantID = grant.AccessGrantID
		if ev.AccessGrantID == "" {
			ev.AccessGrantID = grant.EventID
		}
	}
	if ev.RequestID == "" {
		ev.RequestID = grant.RequestID
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
	if ev.ActorEmail == "" {
		ev.ActorEmail = grant.ActorEmail
	}
	if ev.ActorSubject == "" {
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
