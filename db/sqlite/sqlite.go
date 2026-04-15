package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
	_ "github.com/mattn/go-sqlite3"
)

// SqliteDB implements DatabaseInterface
var _ core.DatabaseInterface = (*SqliteDB)(nil)

type SqliteDB struct {
	db *sql.DB
}

func NewSqliteDB(dsn string) (*SqliteDB, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// Keep a single connection so in-memory SQLite databases remain consistent
	// across schema initialization and subsequent queries.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	s := &SqliteDB{db: db}
	if err := s.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to init schema: %w", err)
	}

	return s, nil
}

func (db *SqliteDB) initSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS drs_object (
			id TEXT PRIMARY KEY,
			size INTEGER,
			created_time TIMESTAMP,
			updated_time TIMESTAMP,
			name TEXT,
			version TEXT,
			description TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS drs_object_access_method (
			object_id TEXT,
			url TEXT,
			type TEXT,
			FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS drs_object_checksum (
			object_id TEXT,
			type TEXT,
			checksum TEXT,
			FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS drs_object_authz (
			object_id TEXT,
			resource TEXT,
			FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS drs_object_alias (
			alias_id TEXT PRIMARY KEY,
			object_id TEXT NOT NULL,
			FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_drs_object_alias_object_id ON drs_object_alias(object_id)`,
		`CREATE TABLE IF NOT EXISTS s3_credential (
			bucket TEXT PRIMARY KEY,
			provider TEXT NOT NULL DEFAULT 's3',
			region TEXT,
			access_key TEXT,
			secret_key TEXT,
			endpoint TEXT
		)`,
		`CREATE TABLE IF NOT EXISTS bucket_scope (
			organization TEXT NOT NULL,
			project_id TEXT NOT NULL,
			bucket TEXT NOT NULL,
			path_prefix TEXT,
			PRIMARY KEY (organization, project_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_bucket_scope_bucket ON bucket_scope(bucket)`,
		`CREATE TABLE IF NOT EXISTS lfs_pending_metadata (
			oid TEXT PRIMARY KEY,
			candidate_json TEXT NOT NULL,
			created_time TIMESTAMP NOT NULL,
			expires_time TIMESTAMP NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_expires ON lfs_pending_metadata(expires_time)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_created ON lfs_pending_metadata(created_time)`,
		`CREATE TABLE IF NOT EXISTS object_usage (
			object_id TEXT PRIMARY KEY,
			upload_count INTEGER NOT NULL DEFAULT 0,
			download_count INTEGER NOT NULL DEFAULT 0,
			last_upload_time TIMESTAMP NULL,
			last_download_time TIMESTAMP NULL,
			updated_time TIMESTAMP NOT NULL,
			FOREIGN KEY(object_id) REFERENCES drs_object(id) ON DELETE CASCADE
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_download_time ON object_usage(last_download_time)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_upload_time ON object_usage(last_upload_time)`,
		`CREATE TABLE IF NOT EXISTS object_usage_event (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			object_id TEXT NOT NULL,
			event_type TEXT NOT NULL CHECK(event_type IN ('upload','download')),
			event_time TIMESTAMP NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_object_id ON object_usage_event(object_id)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_event_time ON object_usage_event(event_time)`,
	}

	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return err
		}
	}
	if _, err := db.db.Exec(`ALTER TABLE s3_credential ADD COLUMN provider TEXT NOT NULL DEFAULT 's3'`); err != nil {
		if !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
			return err
		}
	}
	return nil
}

func (db *SqliteDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	name := "Calypr-backed DRS server (SQLite)"
	createdAt := time.Now()
	updatedAt := time.Now()
	environment := "prod"
	return &drs.Service{
		Id:          "drs-service-calypr",
		Name:        "Calypr DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: &name,
		CreatedAt:   &createdAt,
		UpdatedAt:   &updatedAt,
		Environment: &environment,
		Version:     "1.0.0",
	}, nil
}
