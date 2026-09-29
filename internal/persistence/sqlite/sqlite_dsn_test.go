package sqlite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSqliteDSNLeavesOptionNamesInFilenameUntouched(t *testing.T) {
	for _, name := range []string{"cache_txlock=deferred.db", "cache_foreign_keys=off.db"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), name)
			db, err := NewSqliteDB(path, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("database opened a different path: %v", err)
			}
			var enabled int
			if err := db.DB().QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil {
				t.Fatal(err)
			}
			if enabled != 1 {
				t.Fatalf("foreign keys = %d, want 1", enabled)
			}
		})
	}
}

func TestSqliteDSNOverridesOnlyTxLockQueryOption(t *testing.T) {
	got := sqliteDSN("file:test.db?cache=shared&_txlock=deferred&_foreign_keys=off")
	want := "file:test.db?cache=shared&_txlock=immediate&_foreign_keys=off"
	if got != want {
		t.Fatalf("sqliteDSN = %q, want %q", got, want)
	}
}
