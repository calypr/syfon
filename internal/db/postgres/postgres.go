package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	sycommon "github.com/calypr/syfon/common"

	// Postgres driver
	_ "github.com/lib/pq"
)

// PostgresDB implements DatabaseInterface
type PostgresDB struct {
	db *sql.DB
}

func NewPostgresDB(dsn string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	pg := &PostgresDB{db: db}
	if err := pg.ensureObjectSchema(); err != nil {
		return nil, err
	}
	if err := pg.normalizeControlledAccessResources(context.Background()); err != nil {
		return nil, err
	}
	if err := pg.ensureBucketScopeSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureS3CredentialSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureLFSPendingSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureObjectUsageSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensurePendingObjectUsageSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureTransferAttributionSchema(); err != nil {
		return nil, err
	}
	return pg, nil
}

func (db *PostgresDB) normalizeControlledAccessResources(ctx context.Context) error {
	rows, err := db.db.QueryContext(ctx, `SELECT object_id, resource FROM drs_object_controlled_access`)
	if err != nil {
		return fmt.Errorf("failed to scan controlled access resources: %w", err)
	}
	defer rows.Close()

	type rewrite struct {
		objectID string
		old      string
		new      string
	}
	rewrites := []rewrite{}
	for rows.Next() {
		var objectID, resource string
		if err := rows.Scan(&objectID, &resource); err != nil {
			return fmt.Errorf("failed to scan controlled access resource row: %w", err)
		}
		normalized := sycommon.NormalizeAccessResource(resource)
		if normalized == "" || normalized == resource {
			continue
		}
		rewrites = append(rewrites, rewrite{objectID: objectID, old: resource, new: normalized})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed while scanning controlled access resources: %w", err)
	}
	if len(rewrites) == 0 {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin controlled access resource normalization: %w", err)
	}
	defer tx.Rollback()
	for _, rw := range rewrites {
		if _, err := tx.ExecContext(ctx, `
			DELETE FROM drs_object_controlled_access
			WHERE object_id = $1 AND resource = $2`, rw.objectID, rw.new); err != nil {
			return fmt.Errorf("remove duplicate normalized controlled access resource: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE drs_object_controlled_access
			SET resource = $3
			WHERE object_id = $1 AND resource = $2`, rw.objectID, rw.old, rw.new); err != nil {
			return fmt.Errorf("rewrite controlled access resource: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit controlled access resource normalization: %w", err)
	}
	return nil
}

func (db *PostgresDB) ensureObjectSchema() error {
	queries, err := objectSchemaStatements()
	if err != nil {
		return fmt.Errorf("failed to load object schema: %w", err)
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureS3CredentialSchema() error {
	_, err := db.db.Exec(`
		ALTER TABLE s3_credential
		ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 's3'
	`)
	if err != nil {
		return fmt.Errorf("failed to initialize s3_credential provider schema: %w", err)
	}
	for _, stmt := range []string{
		`ALTER TABLE s3_credential ADD COLUMN IF NOT EXISTS billing_log_bucket TEXT`,
		`ALTER TABLE s3_credential ADD COLUMN IF NOT EXISTS billing_log_prefix TEXT`,
	} {
		if _, err := db.db.Exec(stmt); err != nil {
			return fmt.Errorf("failed to initialize s3_credential billing log schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureBucketScopeSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS bucket_scope (
			organization TEXT NOT NULL,
			project_id TEXT NOT NULL,
			bucket TEXT NOT NULL,
			path_prefix TEXT NULL,
			PRIMARY KEY (organization, project_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_bucket_scope_bucket ON bucket_scope(bucket)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize bucket scope schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureLFSPendingSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS lfs_pending_metadata (
			oid TEXT PRIMARY KEY,
			candidate_json JSONB NOT NULL,
			created_time TIMESTAMPTZ NOT NULL,
			expires_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_expires ON lfs_pending_metadata(expires_time)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_created ON lfs_pending_metadata(created_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize lfs pending metadata schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureObjectUsageSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS object_usage (
			object_id TEXT PRIMARY KEY REFERENCES drs_object(id) ON DELETE CASCADE,
			upload_count BIGINT NOT NULL DEFAULT 0,
			download_count BIGINT NOT NULL DEFAULT 0,
			last_upload_time TIMESTAMPTZ NULL,
			last_download_time TIMESTAMPTZ NULL,
			updated_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_download_time ON object_usage(last_download_time)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_download_time_object_id ON object_usage(last_download_time, object_id)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_upload_time ON object_usage(last_upload_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object usage schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensurePendingObjectUsageSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS object_usage_event (
			id BIGSERIAL PRIMARY KEY,
			object_id TEXT NOT NULL,
			event_type TEXT NOT NULL CHECK (event_type IN ('upload','download')),
			event_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_object_id ON object_usage_event(object_id)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_event_time ON object_usage_event(event_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object usage event schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureTransferAttributionSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS transfer_attribution_event (
			event_id TEXT PRIMARY KEY,
			access_grant_id TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL CHECK (event_type IN ('access_issued')),
			direction TEXT NOT NULL DEFAULT 'download' CHECK (direction IN ('download','upload')),
			event_time TIMESTAMPTZ NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			object_id TEXT NOT NULL DEFAULT '',
			sha256 TEXT NOT NULL DEFAULT '',
			object_size BIGINT NOT NULL DEFAULT 0,
			organization TEXT NOT NULL DEFAULT '',
			project TEXT NOT NULL DEFAULT '',
			access_id TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			bucket TEXT NOT NULL DEFAULT '',
			storage_url TEXT NOT NULL DEFAULT '',
			range_start BIGINT NULL,
			range_end BIGINT NULL,
			bytes_requested BIGINT NOT NULL DEFAULT 0,
			bytes_completed BIGINT NOT NULL DEFAULT 0,
			actor_email TEXT NOT NULL DEFAULT '',
			actor_subject TEXT NOT NULL DEFAULT '',
			auth_mode TEXT NOT NULL DEFAULT '',
			client_name TEXT NOT NULL DEFAULT '',
			client_version TEXT NOT NULL DEFAULT '',
			transfer_session_id TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS access_grant (
			access_grant_id TEXT PRIMARY KEY,
			first_issued_at TIMESTAMPTZ NOT NULL,
			last_issued_at TIMESTAMPTZ NOT NULL,
			issue_count BIGINT NOT NULL DEFAULT 0,
			object_id TEXT NOT NULL DEFAULT '',
			sha256 TEXT NOT NULL DEFAULT '',
			object_size BIGINT NOT NULL DEFAULT 0,
			organization TEXT NOT NULL DEFAULT '',
			project TEXT NOT NULL DEFAULT '',
			access_id TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			bucket TEXT NOT NULL DEFAULT '',
			storage_url TEXT NOT NULL DEFAULT '',
			actor_email TEXT NOT NULL DEFAULT '',
			actor_subject TEXT NOT NULL DEFAULT '',
			auth_mode TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS provider_transfer_event (
			provider_event_id TEXT PRIMARY KEY,
			access_grant_id TEXT NOT NULL DEFAULT '',
			direction TEXT NOT NULL CHECK (direction IN ('download','upload')),
			event_time TIMESTAMPTZ NOT NULL,
			request_id TEXT NOT NULL DEFAULT '',
			provider_request_id TEXT NOT NULL DEFAULT '',
			object_id TEXT NOT NULL DEFAULT '',
			sha256 TEXT NOT NULL DEFAULT '',
			object_size BIGINT NOT NULL DEFAULT 0,
			organization TEXT NOT NULL DEFAULT '',
			project TEXT NOT NULL DEFAULT '',
			access_id TEXT NOT NULL DEFAULT '',
			provider TEXT NOT NULL DEFAULT '',
			bucket TEXT NOT NULL DEFAULT '',
			object_key TEXT NOT NULL DEFAULT '',
			storage_url TEXT NOT NULL DEFAULT '',
			range_start BIGINT NULL,
			range_end BIGINT NULL,
			bytes_transferred BIGINT NOT NULL DEFAULT 0,
			http_method TEXT NOT NULL DEFAULT '',
			http_status INTEGER NOT NULL DEFAULT 0,
			requester_principal TEXT NOT NULL DEFAULT '',
			source_ip TEXT NOT NULL DEFAULT '',
			user_agent TEXT NOT NULL DEFAULT '',
			raw_event_ref TEXT NOT NULL DEFAULT '',
			actor_email TEXT NOT NULL DEFAULT '',
			actor_subject TEXT NOT NULL DEFAULT '',
			auth_mode TEXT NOT NULL DEFAULT '',
			reconciliation_status TEXT NOT NULL DEFAULT 'unmatched' CHECK (reconciliation_status IN ('matched','ambiguous','unmatched'))
		)`,
		`ALTER TABLE transfer_attribution_event ADD COLUMN IF NOT EXISTS access_grant_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE transfer_attribution_event ADD COLUMN IF NOT EXISTS direction TEXT NOT NULL DEFAULT 'download'`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_scope_time ON transfer_attribution_event(organization, project, event_type, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_scope_event_time ON transfer_attribution_event(organization, project, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_direction_time ON transfer_attribution_event(direction, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_actor_time ON transfer_attribution_event(actor_email, actor_subject, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_provider_time ON transfer_attribution_event(provider, bucket, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_sha_time ON transfer_attribution_event(sha256, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_transfer_attr_session ON transfer_attribution_event(transfer_session_id)`,
		`CREATE INDEX IF NOT EXISTS idx_access_grant_storage_time ON access_grant(provider, bucket, storage_url, last_issued_at)`,
		`CREATE INDEX IF NOT EXISTS idx_access_grant_scope_time ON access_grant(organization, project, last_issued_at)`,
		`CREATE INDEX IF NOT EXISTS idx_access_grant_sha_time ON access_grant(sha256, last_issued_at)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_scope_time ON provider_transfer_event(organization, project, direction, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_actor_time ON provider_transfer_event(actor_email, actor_subject, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_provider_time ON provider_transfer_event(provider, bucket, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_sha_time ON provider_transfer_event(sha256, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_status ON provider_transfer_event(reconciliation_status, event_time)`,
		`CREATE INDEX IF NOT EXISTS idx_provider_transfer_grant ON provider_transfer_event(access_grant_id)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize transfer attribution schema: %w", err)
		}
	}
	if err := db.backfillAccessGrants(context.Background()); err != nil {
		return fmt.Errorf("failed to backfill access grants: %w", err)
	}
	return nil
}

func (db *PostgresDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	// Static info for now, or fetch from DB if stored there
	description := "Calypr-backed DRS server"
	createdAt := time.Now()
	updatedAt := time.Now()
	environment := "prod"
	return &drs.Service{
		Id:          "drs-service-calypr",
		Name:        "Calypr DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: &description,
		CreatedAt:   &createdAt,
		UpdatedAt:   &updatedAt,
		Environment: &environment,
		Version:     "1.0.0",
	}, nil
}
