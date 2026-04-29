package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDump struct {
	db *sql.DB
}

func OpenSQLiteDump(path string) (*SQLiteDump, error) {
	if path == "" {
		return nil, fmt.Errorf("dump path is required")
	}
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	dump := &SQLiteDump{db: db}
	if err := dump.init(); err != nil {
		db.Close()
		return nil, err
	}
	return dump, nil
}

func OpenExistingSQLiteDump(path string) (*SQLiteDump, error) {
	if path == "" {
		return nil, fmt.Errorf("dump path is required")
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	return OpenSQLiteDump(path)
}

func (d *SQLiteDump) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

func (d *SQLiteDump) init() error {
	_, err := d.db.Exec(`
CREATE TABLE IF NOT EXISTS migration_records (
  id TEXT PRIMARY KEY,
  record_json TEXT NOT NULL,
  exported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS migration_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`)
	return err
}

func (d *SQLiteDump) LoadBatch(ctx context.Context, records []MigrationRecord) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO migration_records (id, record_json, exported_at)
VALUES (?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  record_json=excluded.record_json,
  exported_at=excluded.exported_at
`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, record := range records {
		body, err := json.Marshal(record)
		if err != nil {
			return err
		}
		if _, err := stmt.ExecContext(ctx, record.ID, string(body), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *SQLiteDump) ReadBatches(ctx context.Context, batchSize int, fn func([]MigrationRecord) error) error {
	if batchSize <= 0 {
		batchSize = 500
	}
	offset := 0
	for {
		rows, err := d.db.QueryContext(ctx, `
SELECT record_json FROM migration_records
ORDER BY id
LIMIT ? OFFSET ?
`, batchSize, offset)
		if err != nil {
			return err
		}

		batch := make([]MigrationRecord, 0, batchSize)
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				rows.Close()
				return err
			}
			var record MigrationRecord
			if err := json.Unmarshal([]byte(raw), &record); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, record)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		if len(batch) < batchSize {
			return nil
		}
		offset += len(batch)
	}
}

func (d *SQLiteDump) Count(ctx context.Context) (int, error) {
	var count int
	err := d.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM migration_records`).Scan(&count)
	return count, err
}
