package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/calypr/syfon/internal/persistence/credentialcipher"
	"github.com/calypr/syfon/internal/persistence/store"

	// Postgres driver
	_ "github.com/lib/pq"
)

func NewPostgresDB(ctx context.Context, dsn string, cipher store.CredentialCodec) (*store.Store, error) {
	return NewPostgresDBWithOptions(ctx, dsn, cipher, OpenOptions{})
}

type SchemaMode string

const (
	SchemaModeAuto  SchemaMode = "auto"
	SchemaModeCheck SchemaMode = "check"
)

type OpenOptions struct {
	SchemaMode             SchemaMode
	MaxOpenConnections     int
	MaxIdleConnections     int
	ConnectionMaxLifetime  time.Duration
	ConnectionMaxIdleTime  time.Duration
	PingTimeout            time.Duration
	SchemaBootstrapTimeout time.Duration
}

const defaultSchemaBootstrapTimeout = 30 * time.Second

func NewPostgresDBWithOptions(ctx context.Context, dsn string, cipher store.CredentialCodec, options OpenOptions) (*store.Store, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	mode := options.SchemaMode
	if mode == "" {
		mode = SchemaModeAuto
		options.SchemaMode = mode
	}
	var err error
	if cipher == nil && mode == SchemaModeAuto {
		cipher, err = credentialcipher.NewFromEnv()
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if options.MaxOpenConnections > 0 {
		db.SetMaxOpenConns(options.MaxOpenConnections)
	}
	if options.MaxIdleConnections > 0 {
		db.SetMaxIdleConns(options.MaxIdleConnections)
	}
	if options.ConnectionMaxLifetime > 0 {
		db.SetConnMaxLifetime(options.ConnectionMaxLifetime)
	}
	if options.ConnectionMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(options.ConnectionMaxIdleTime)
	}
	return openPostgresStore(ctx, db, cipher, options)
}

func openPostgresStore(ctx context.Context, db *sql.DB, cipher store.CredentialCodec, options OpenOptions) (*store.Store, error) {
	mode := options.SchemaMode
	if mode == "" {
		mode = SchemaModeAuto
	}
	pingCtx := ctx
	var cancel context.CancelFunc
	if options.PingTimeout > 0 {
		pingCtx, cancel = context.WithTimeout(ctx, options.PingTimeout)
		defer cancel()
	}
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	schemaCtx := ctx
	var schemaCancel context.CancelFunc
	if options.PingTimeout > 0 {
		schemaCtx, schemaCancel = context.WithTimeout(ctx, options.PingTimeout)
		defer schemaCancel()
	}
	switch mode {
	case SchemaModeCheck:
		if err := CheckSchema(schemaCtx, db); err != nil {
			_ = db.Close()
			return nil, err
		}
	case SchemaModeAuto:
		bootstrapTimeout := options.SchemaBootstrapTimeout
		if bootstrapTimeout <= 0 {
			bootstrapTimeout = defaultSchemaBootstrapTimeout
		}
		bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
		defer bootstrapCancel()
		shared, err := store.Open(bootstrapCtx, db, postgresDialect{}, cipher)
		if err != nil {
			return nil, err
		}
		return shared, nil
	default:
		_ = db.Close()
		return nil, fmt.Errorf("invalid postgres schema mode %q", mode)
	}
	return store.OpenPrepared(db, postgresDialect{}, cipher)
}
