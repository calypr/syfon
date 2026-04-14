package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"

	// Postgres driver
	_ "github.com/lib/pq"
)

// PostgresDB implements DatabaseInterface
var _ core.DatabaseInterface = (*PostgresDB)(nil)

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
	if err := pg.ensureObjectAliasSchema(); err != nil {
		return nil, err
	}
	return pg, nil
}

func (db *PostgresDB) ensureS3CredentialSchema() error {
	_, err := db.db.Exec(`
		ALTER TABLE s3_credential
		ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 's3'
	`)
	if err != nil {
		return fmt.Errorf("failed to initialize s3_credential provider schema: %w", err)
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

func (db *PostgresDB) ensureObjectAliasSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS drs_object_alias (
			alias_id TEXT PRIMARY KEY,
			object_id TEXT NOT NULL REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_drs_object_alias_object_id ON drs_object_alias(object_id)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object alias schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	// Static info for now, or fetch from DB if stored there
	return &drs.Service{
		Id:          "drs-service-calypr",
		Name:        "Calypr DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: "Calypr-backed DRS server",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Environment: "prod",
		Version:     "1.0.0",
	}, nil
}
