package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"

	"github.com/lib/pq"
)

func (db *PostgresDB) RecordFileUpload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES ($1, 'upload', $2)
	`, objectID, now)
	return err
}

func (db *PostgresDB) RecordFileDownload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES ($1, 'download', $2)
	`, objectID, now)
	return err
}

func (db *PostgresDB) RecordTransferAttributionEvents(ctx context.Context, events []models.TransferAttributionEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO transfer_attribution_event (
			event_id, access_grant_id, event_type, event_time, request_id, object_id, sha256, object_size,
			organization, project, access_id, provider, bucket, storage_url,
			range_start, range_end, bytes_requested, bytes_completed,
			actor_email, actor_subject, auth_mode, client_name, client_version, transfer_session_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
		ON CONFLICT (event_id) DO NOTHING
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
		result, err := stmt.ExecContext(ctx,
			ev.EventID, ev.AccessGrantID, ev.EventType, ev.EventTime, ev.RequestID, ev.ObjectID, ev.SHA256, ev.ObjectSize,
			ev.Organization, ev.Project, ev.AccessID, ev.Provider, ev.Bucket, ev.StorageURL,
			nullableInt64(ev.RangeStart), nullableInt64(ev.RangeEnd), ev.BytesRequested, ev.BytesCompleted,
			ev.ActorEmail, ev.ActorSubject, ev.AuthMode, ev.ClientName, ev.ClientVersion, ev.TransferSessionID,
		)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err == nil && rows > 0 {
			if err := postgresUpsertAccessGrant(ctx, tx, ev); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (db *PostgresDB) RecordProviderTransferEvents(ctx context.Context, events []models.ProviderTransferEvent) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO provider_transfer_event (
			provider_event_id, access_grant_id, direction, event_time, request_id, provider_request_id,
			object_id, sha256, object_size, organization, project, access_id, provider, bucket,
			object_key, storage_url, range_start, range_end, bytes_transferred, http_method, http_status,
			requester_principal, source_ip, user_agent, raw_event_ref, actor_email, actor_subject, auth_mode,
			reconciliation_status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
		ON CONFLICT (provider_event_id) DO NOTHING
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

func (db *PostgresDB) RecordProviderTransferSyncRuns(ctx context.Context, runs []models.ProviderTransferSyncRun) error {
	if len(runs) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO provider_transfer_sync_run (
			sync_id, provider, bucket, organization, project, from_time, to_time, status,
			requested_at, started_at, completed_at, imported_events, matched_events,
			ambiguous_events, unmatched_events, error_message
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (sync_id) DO UPDATE SET
			provider = EXCLUDED.provider,
			bucket = EXCLUDED.bucket,
			organization = EXCLUDED.organization,
			project = EXCLUDED.project,
			from_time = EXCLUDED.from_time,
			to_time = EXCLUDED.to_time,
			status = EXCLUDED.status,
			requested_at = EXCLUDED.requested_at,
			started_at = EXCLUDED.started_at,
			completed_at = EXCLUDED.completed_at,
			imported_events = EXCLUDED.imported_events,
			matched_events = EXCLUDED.matched_events,
			ambiguous_events = EXCLUDED.ambiguous_events,
			unmatched_events = EXCLUDED.unmatched_events,
			error_message = EXCLUDED.error_message
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

func (db *PostgresDB) ListProviderTransferSyncRuns(ctx context.Context, filter models.TransferAttributionFilter, limit int) ([]models.ProviderTransferSyncRun, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	where, args := providerSyncWhere(filter)
	args = append(args, limit)
	query := fmt.Sprintf(`
		SELECT sync_id, provider, bucket, organization, project, from_time, to_time, status,
			requested_at, started_at, completed_at, imported_events, matched_events,
			ambiguous_events, unmatched_events, error_message
		FROM provider_transfer_sync_run%s
		ORDER BY requested_at DESC, sync_id ASC
		LIMIT $%d`, where, len(args))
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProviderSyncRuns(rows)
}

func (db *PostgresDB) GetTransferAttributionSummary(ctx context.Context, filter models.TransferAttributionFilter) (models.TransferAttributionSummary, error) {
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

func (db *PostgresDB) GetTransferAttributionBreakdown(ctx context.Context, filter models.TransferAttributionFilter, groupBy string) ([]models.TransferAttributionBreakdown, error) {
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

func (db *PostgresDB) GetFileUsage(ctx context.Context, objectID string) (*models.FileUsage, error) {
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
		WHERE o.id = $1
	`, objectID).Scan(
		&usage.ObjectID,
		&usage.Name,
		&usage.Size,
		&usage.UploadCount,
		&usage.DownloadCount,
		&lastUpload,
		&lastDownload,
	)
	if err == sql.ErrNoRows {
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
	usage.LastAccessTime = latestUsageTime(usage.LastUploadTime, usage.LastDownloadTime)
	return &usage, nil
}

func (db *PostgresDB) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, error) {
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
		query += ` WHERE u.last_download_time IS NULL OR u.last_download_time < $1`
		args = append(args, inactiveSince.UTC())
	}
	query += fmt.Sprintf(` ORDER BY COALESCE(u.last_download_time, '1970-01-01T00:00:00Z') ASC, o.id ASC LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2)
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
		usage.LastAccessTime = latestUsageTime(usage.LastUploadTime, usage.LastDownloadTime)
		out = append(out, usage)
	}
	return out, nil
}

func (db *PostgresDB) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (models.FileUsageSummary, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return models.FileUsageSummary{}, err
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -730)
	if inactiveSince != nil {
		cutoff = inactiveSince.UTC()
	}
	var summary models.FileUsageSummary
	if err := db.db.QueryRowContext(ctx, `
		SELECT
			COUNT(o.id) AS total_files,
			COALESCE(SUM(COALESCE(u.upload_count, 0)), 0) AS total_uploads,
			COALESCE(SUM(COALESCE(u.download_count, 0)), 0) AS total_downloads,
			COALESCE(SUM(CASE WHEN u.last_download_time IS NULL OR u.last_download_time < $1 THEN 1 ELSE 0 END), 0) AS inactive_files
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
	`, cutoff).Scan(
		&summary.TotalFiles,
		&summary.TotalUploads,
		&summary.TotalDownloads,
		&summary.InactiveFileCount,
	); err != nil {
		return models.FileUsageSummary{}, err
	}
	return summary, nil
}

func (db *PostgresDB) flushObjectUsageEvents(ctx context.Context) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ids, err := db.existingObjectIDsWithEvents(ctx, tx)
	if err != nil {
		return err
	}
	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *PostgresDB) existingObjectIDsWithEvents(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (db *PostgresDB) flushObjectUsageEventsForIDsTx(ctx context.Context, tx *sql.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO object_usage (object_id, upload_count, download_count, last_upload_time, last_download_time, updated_time)
		SELECT e.object_id,
			COALESCE(SUM(CASE WHEN e.event_type = 'upload' THEN 1 ELSE 0 END), 0) AS upload_count,
			COALESCE(SUM(CASE WHEN e.event_type = 'download' THEN 1 ELSE 0 END), 0) AS download_count,
			MAX(CASE WHEN e.event_type = 'upload' THEN e.event_time END) AS last_upload_time,
			MAX(CASE WHEN e.event_type = 'download' THEN e.event_time END) AS last_download_time,
			$2
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
		WHERE e.object_id = ANY($1)
		GROUP BY e.object_id
		ON CONFLICT (object_id) DO UPDATE SET
			upload_count = object_usage.upload_count + EXCLUDED.upload_count,
			download_count = object_usage.download_count + EXCLUDED.download_count,
			last_upload_time = CASE
				WHEN EXCLUDED.last_upload_time IS NULL THEN object_usage.last_upload_time
				WHEN object_usage.last_upload_time IS NULL THEN EXCLUDED.last_upload_time
				WHEN EXCLUDED.last_upload_time > object_usage.last_upload_time THEN EXCLUDED.last_upload_time
				ELSE object_usage.last_upload_time
			END,
			last_download_time = CASE
				WHEN EXCLUDED.last_download_time IS NULL THEN object_usage.last_download_time
				WHEN object_usage.last_download_time IS NULL THEN EXCLUDED.last_download_time
				WHEN EXCLUDED.last_download_time > object_usage.last_download_time THEN EXCLUDED.last_download_time
				ELSE object_usage.last_download_time
			END,
			updated_time = EXCLUDED.updated_time
	`, pq.Array(ids), now); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM object_usage_event e
		USING drs_object o
		WHERE e.object_id = o.id AND e.object_id = ANY($1)
	`, pq.Array(ids))
	return err
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

func (db *PostgresDB) backfillAccessGrants(ctx context.Context) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `
		SELECT event_id, access_grant_id, event_type, event_time, request_id, object_id, sha256, object_size,
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
			&ev.EventID, &ev.AccessGrantID, &ev.EventType, &ev.EventTime, &ev.RequestID, &ev.ObjectID, &ev.SHA256, &ev.ObjectSize,
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
		if _, err := tx.ExecContext(ctx, `UPDATE transfer_attribution_event SET access_grant_id = $1 WHERE event_id = $2`, ev.AccessGrantID, ev.EventID); err != nil {
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
			INSERT INTO access_grant (
				access_grant_id, first_issued_at, last_issued_at, issue_count,
				object_id, sha256, object_size, organization, project, access_id,
				provider, bucket, storage_url, actor_email, actor_subject, auth_mode
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			ON CONFLICT (access_grant_id) DO NOTHING
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

func providerSyncWhere(filter models.TransferAttributionFilter) (string, []any) {
	parts := make([]string, 0)
	args := make([]any, 0)
	add := func(column string, value any) {
		args = append(args, value)
		parts = append(parts, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if strings.TrimSpace(filter.Organization) != "" {
		add("organization", strings.TrimSpace(filter.Organization))
	}
	if strings.TrimSpace(filter.Project) != "" {
		add("project", strings.TrimSpace(filter.Project))
	}
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket", strings.TrimSpace(filter.Bucket))
	}
	if filter.From != nil {
		args = append(args, filter.From.UTC())
		parts = append(parts, fmt.Sprintf("to_time >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, filter.To.UTC())
		parts = append(parts, fmt.Sprintf("from_time <= $%d", len(args)))
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

func (db *PostgresDB) reconcileProviderTransferEvent(ctx context.Context, tx *sql.Tx, ev models.ProviderTransferEvent) (models.ProviderTransferEvent, error) {
	ev.Direction = normalizeProviderDirection(ev.Direction, ev.HTTPMethod)
	ev.Provider = strings.TrimSpace(ev.Provider)
	ev.Bucket = strings.TrimSpace(ev.Bucket)
	ev.ObjectKey = strings.TrimLeft(strings.TrimSpace(ev.ObjectKey), "/")
	ev.StorageURL = strings.TrimSpace(ev.StorageURL)
	ev.ReconciliationStatus = models.ProviderTransferUnmatched
	if ev.AccessGrantID != "" {
		if match, ok, err := postgresAccessGrantByID(ctx, tx, ev.AccessGrantID); err != nil {
			return ev, err
		} else if ok {
			mergeAccessGrantIntoProviderEvent(&ev, match)
			ev.ReconciliationStatus = models.ProviderTransferMatched
			return ev, nil
		}
	}
	matches, err := postgresAccessGrantCandidates(ctx, tx, ev)
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

func postgresUpsertAccessGrant(ctx context.Context, tx *sql.Tx, ev models.TransferAttributionEvent) error {
	if ev.AccessGrantID == "" {
		return nil
	}
	when := ev.EventTime.UTC()
	_, err := tx.ExecContext(ctx, `
		INSERT INTO access_grant (
			access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		) VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (access_grant_id) DO UPDATE SET
			first_issued_at = LEAST(access_grant.first_issued_at, EXCLUDED.first_issued_at),
			last_issued_at = GREATEST(access_grant.last_issued_at, EXCLUDED.last_issued_at),
			issue_count = access_grant.issue_count + 1,
			object_id = COALESCE(NULLIF(access_grant.object_id, ''), EXCLUDED.object_id),
			sha256 = COALESCE(NULLIF(access_grant.sha256, ''), EXCLUDED.sha256),
			object_size = CASE WHEN access_grant.object_size = 0 THEN EXCLUDED.object_size ELSE access_grant.object_size END,
			organization = COALESCE(NULLIF(access_grant.organization, ''), EXCLUDED.organization),
			project = COALESCE(NULLIF(access_grant.project, ''), EXCLUDED.project),
			access_id = COALESCE(NULLIF(access_grant.access_id, ''), EXCLUDED.access_id),
			provider = COALESCE(NULLIF(access_grant.provider, ''), EXCLUDED.provider),
			bucket = COALESCE(NULLIF(access_grant.bucket, ''), EXCLUDED.bucket),
			storage_url = COALESCE(NULLIF(access_grant.storage_url, ''), EXCLUDED.storage_url),
			actor_email = COALESCE(NULLIF(access_grant.actor_email, ''), EXCLUDED.actor_email),
			actor_subject = COALESCE(NULLIF(access_grant.actor_subject, ''), EXCLUDED.actor_subject),
			auth_mode = COALESCE(NULLIF(access_grant.auth_mode, ''), EXCLUDED.auth_mode)
	`, ev.AccessGrantID, when, when, ev.ObjectID, ev.SHA256, ev.ObjectSize,
		ev.Organization, ev.Project, ev.AccessID, ev.Provider, ev.Bucket, ev.StorageURL,
		ev.ActorEmail, ev.ActorSubject, ev.AuthMode)
	return err
}

func postgresAccessGrantByID(ctx context.Context, tx *sql.Tx, grantID string) (models.AccessGrant, bool, error) {
	var grant models.AccessGrant
	err := tx.QueryRowContext(ctx, `
		SELECT access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		FROM access_grant
		WHERE access_grant_id = $1
	`, grantID).Scan(
		&grant.AccessGrantID, &grant.FirstIssuedAt, &grant.LastIssuedAt, &grant.IssueCount,
		&grant.ObjectID, &grant.SHA256, &grant.ObjectSize, &grant.Organization, &grant.Project, &grant.AccessID,
		&grant.Provider, &grant.Bucket, &grant.StorageURL, &grant.ActorEmail, &grant.ActorSubject, &grant.AuthMode,
	)
	if err == sql.ErrNoRows {
		return models.AccessGrant{}, false, nil
	}
	return grant, err == nil, err
}

func postgresAccessGrantCandidates(ctx context.Context, tx *sql.Tx, ev models.ProviderTransferEvent) ([]models.AccessGrant, error) {
	args := []any{ev.Provider, ev.Bucket, ev.EventTime.UTC().Add(15 * time.Minute), ev.EventTime.UTC().Add(-24 * time.Hour)}
	query := `
		SELECT access_grant_id, first_issued_at, last_issued_at, issue_count,
			object_id, sha256, object_size, organization, project, access_id,
			provider, bucket, storage_url, actor_email, actor_subject, auth_mode
		FROM access_grant
		WHERE provider = $1
			AND bucket = $2
			AND last_issued_at <= $3
			AND last_issued_at >= $4
	`
	if ev.StorageURL != "" {
		args = append(args, ev.StorageURL)
		query += fmt.Sprintf(" AND storage_url = $%d", len(args))
	} else if ev.ObjectKey != "" {
		args = append(args, providerStorageURL(ev.Provider, ev.Bucket, ev.ObjectKey), "%/"+ev.ObjectKey)
		query += fmt.Sprintf(" AND (storage_url = $%d OR storage_url LIKE $%d)", len(args)-1, len(args))
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
	add := func(column string, value any) {
		args = append(args, value)
		parts = append(parts, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	if strings.TrimSpace(filter.Organization) != "" {
		add("organization", strings.TrimSpace(filter.Organization))
	}
	if strings.TrimSpace(filter.Project) != "" {
		add("project", strings.TrimSpace(filter.Project))
	}
	if strings.TrimSpace(filter.EventType) != "" && strings.TrimSpace(filter.EventType) != "all" {
		add("event_type", strings.TrimSpace(filter.EventType))
	}
	if filter.From != nil {
		args = append(args, filter.From.UTC())
		parts = append(parts, fmt.Sprintf("event_time >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, filter.To.UTC())
		parts = append(parts, fmt.Sprintf("event_time <= $%d", len(args)))
	}
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket", strings.TrimSpace(filter.Bucket))
	}
	if strings.TrimSpace(filter.SHA256) != "" {
		add("sha256", strings.TrimSpace(filter.SHA256))
	}
	if strings.TrimSpace(filter.User) != "" {
		user := strings.TrimSpace(filter.User)
		args = append(args, user, user)
		parts = append(parts, fmt.Sprintf("(actor_email = $%d OR actor_subject = $%d)", len(args)-1, len(args)))
	}
	if len(parts) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(parts, " AND "), args
}

func providerTransferWhere(filter models.TransferAttributionFilter) (string, []any) {
	parts := make([]string, 0)
	args := make([]any, 0)
	add := func(column string, value any) {
		args = append(args, value)
		parts = append(parts, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	status := strings.TrimSpace(filter.ReconciliationStatus)
	if status == "" {
		status = models.ProviderTransferMatched
	}
	if status != "all" {
		add("reconciliation_status", status)
	}
	if strings.TrimSpace(filter.Organization) != "" {
		add("organization", strings.TrimSpace(filter.Organization))
	}
	if strings.TrimSpace(filter.Project) != "" {
		add("project", strings.TrimSpace(filter.Project))
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
		add("direction", direction)
	}
	if filter.From != nil {
		args = append(args, filter.From.UTC())
		parts = append(parts, fmt.Sprintf("event_time >= $%d", len(args)))
	}
	if filter.To != nil {
		args = append(args, filter.To.UTC())
		parts = append(parts, fmt.Sprintf("event_time <= $%d", len(args)))
	}
	if strings.TrimSpace(filter.Provider) != "" {
		add("provider", strings.TrimSpace(filter.Provider))
	}
	if strings.TrimSpace(filter.Bucket) != "" {
		add("bucket", strings.TrimSpace(filter.Bucket))
	}
	if strings.TrimSpace(filter.SHA256) != "" {
		add("sha256", strings.TrimSpace(filter.SHA256))
	}
	if strings.TrimSpace(filter.User) != "" {
		user := strings.TrimSpace(filter.User)
		args = append(args, user, user)
		parts = append(parts, fmt.Sprintf("(actor_email = $%d OR actor_subject = $%d)", len(args)-1, len(args)))
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
		var last sql.NullTime
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
		if last.Valid {
			t := last.Time
			item.LastTransferTime = &t
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
