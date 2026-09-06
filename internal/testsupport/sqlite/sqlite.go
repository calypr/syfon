package sqlite

import (
	"testing"

	"github.com/calypr/syfon/internal/persistence/sqlite"
)

func New(t testing.TB) *sqlite.SqliteDB {
	t.Helper()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("create in-memory SQLite database: %v", err)
	}
	return database
}
