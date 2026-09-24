package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/calypr/syfon/internal/persistence/credentialcipher"
	"github.com/calypr/syfon/internal/persistence/store"
	_ "github.com/mattn/go-sqlite3"
)

func NewSqliteDB(dsn string, cipher store.CredentialCodec) (*store.Store, error) {
	var err error
	if cipher == nil {
		cipher, err = credentialcipher.NewFromEnv()
		if err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite3", sqliteDSN(dsn))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// Keep a single connection so in-memory SQLite databases remain consistent
	// across schema initialization and subsequent queries.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	shared, err := store.Open(context.Background(), db, sqliteDialect{}, cipher)
	if err != nil {
		return nil, err
	}
	return shared, nil
}

func sqliteDSN(dsn string) string {
	if dsn == ":memory:" {
		dsn = "file::memory:"
	}
	path, query, _ := strings.Cut(dsn, "?")
	params := make([]string, 0, 2)
	if query != "" {
		params = strings.Split(query, "&")
	}
	foreignKeys, txLock := false, false
	for i, param := range params {
		key, _, _ := strings.Cut(param, "=")
		switch key {
		case "_foreign_keys":
			foreignKeys = true
		case "_txlock":
			params[i] = "_txlock=immediate"
			txLock = true
		}
	}
	if !foreignKeys {
		params = append(params, "_foreign_keys=on")
	}
	if !txLock {
		params = append(params, "_txlock=immediate")
	}
	return path + "?" + strings.Join(params, "&")
}
