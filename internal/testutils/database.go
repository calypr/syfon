package testutils

import (
	"github.com/calypr/syfon/internal/db/sqlite"
)

// NewInMemoryDB returns a new SQLite backend backed by an in-memory database.
// This is used primarily for testing.
func NewInMemoryDB() *sqlite.SqliteDB {
	// Use SQLite in-memory mode.
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		panic("failed to create in-memory sqlite db: " + err.Error())
	}
	return database
}
