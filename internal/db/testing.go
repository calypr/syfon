package db

// Note: This package `db` now only contains helpers or is transitional.
// The actual implementations are in subpackages.

import (
	"github.com/calypr/syfon/internal/db/sqlite"
)

// NewInMemoryDB returns a new database interface backed by an in-memory SQLite database.
// This is used primarily for testing.
func NewInMemoryDB() DatabaseInterface {
	// Use SQLite in-memory mode
	db, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		panic("failed to create in-memory sqlite db: " + err.Error())
	}
	return db
}
