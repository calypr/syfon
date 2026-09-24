package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/calypr/syfon/internal/persistence/store"
)

var errBootstrapMissingDeadline = errors.New("schema bootstrap received no deadline")

func TestStoreOpenPassesContextToPostgresBootstrap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	db := sql.OpenDB(contextProbeConnector{exec: func(ctx context.Context, _ string) error {
		if _, ok := ctx.Deadline(); !ok {
			return errBootstrapMissingDeadline
		}
		<-ctx.Done()
		return ctx.Err()
	}})
	t.Cleanup(func() { _ = db.Close() })

	_, err := store.Open(ctx, db, postgresDialect{}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("store.Open error = %v, want schema bootstrap deadline", err)
	}
}

func TestPostgresOpenHonorsCanceledCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	db := sql.OpenDB(contextProbeConnector{})
	t.Cleanup(func() { _ = db.Close() })

	_, err := openPostgresStore(ctx, db, nil, OpenOptions{
		SchemaMode:             SchemaModeAuto,
		PingTimeout:            time.Minute,
		SchemaBootstrapTimeout: time.Minute,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("openPostgresStore error = %v, want caller cancellation", err)
	}
}

func TestPostgresOpenBoundsAutomaticSchemaBootstrap(t *testing.T) {
	db := sql.OpenDB(contextProbeConnector{exec: func(ctx context.Context, _ string) error {
		if _, ok := ctx.Deadline(); !ok {
			return errBootstrapMissingDeadline
		}
		<-ctx.Done()
		return ctx.Err()
	}})
	t.Cleanup(func() { _ = db.Close() })

	_, err := openPostgresStore(context.Background(), db, nil, OpenOptions{
		SchemaMode:             SchemaModeAuto,
		PingTimeout:            time.Second,
		SchemaBootstrapTimeout: 10 * time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("openPostgresStore error = %v, want schema bootstrap deadline", err)
	}
}

type contextProbeConnector struct {
	exec func(context.Context, string) error
}

func (c contextProbeConnector) Connect(context.Context) (driver.Conn, error) {
	return contextProbeConn{exec: c.exec}, nil
}

func (contextProbeConnector) Driver() driver.Driver { return contextProbeDriver{} }

type contextProbeDriver struct{}

func (contextProbeDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("use connector")
}

type contextProbeConn struct {
	exec func(context.Context, string) error
}

func (contextProbeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is not supported")
}

func (contextProbeConn) Close() error { return nil }

func (contextProbeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are not supported")
}

func (c contextProbeConn) Ping(context.Context) error { return nil }

func (c contextProbeConn) ExecContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if c.exec != nil {
		if err := c.exec(ctx, query); err != nil {
			return nil, err
		}
	}
	return driver.RowsAffected(0), nil
}

var _ driver.Connector = contextProbeConnector{}
var _ driver.Pinger = contextProbeConn{}
var _ driver.ExecerContext = contextProbeConn{}
