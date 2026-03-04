package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
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
		`CREATE TABLE IF NOT EXISTS s3_credential (
			bucket TEXT PRIMARY KEY,
			region TEXT,
			access_key TEXT,
			secret_key TEXT,
			endpoint TEXT
		)`,
	}

	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (db *SqliteDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	return &drs.Service{
		Id:          "drs-service-calypr",
		Name:        "Calypr DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: "Calypr-backed DRS server (SQLite)",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Environment: "prod",
		Version:     "1.0.0",
	}, nil
}

func (db *SqliteDB) DeleteObject(ctx context.Context, id string) error {
	result, err := db.db.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ?", id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("object not found")
	}
	return nil
}

func (db *SqliteDB) GetObject(ctx context.Context, id string) (*drs.DrsObject, error) {
	// 1. Fetch main record
	var r core.DrsObjectRecord
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = ?`, id).Scan(
		&r.ID, &r.Size, &r.CreatedTime, &r.UpdatedTime, &r.Name, &r.Version, &r.Description,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch record: %w", err)
	}

	obj := &drs.DrsObject{
		Id:          r.ID,
		Size:        r.Size,
		CreatedTime: r.CreatedTime,
		UpdatedTime: r.UpdatedTime,
		Version:     r.Version,
		Description: r.Description,
		Name:        r.Name,
		SelfUri:     "drs://" + r.ID,
	}

	// 2. Fetch Authz (record-level). Read and close before subsequent queries when
	// running with a single sqlite connection.
	authzRows, err := db.db.QueryContext(ctx, "SELECT resource FROM drs_object_authz WHERE object_id = ?", id)
	if err != nil {
		return nil, err
	}
	var recordResources []string
	seenResource := make(map[string]struct{})
	for authzRows.Next() {
		var res string
		if err := authzRows.Scan(&res); err != nil {
			return nil, err
		}
		if _, ok := seenResource[res]; ok {
			continue
		}
		seenResource[res] = struct{}{}
		recordResources = append(recordResources, res)
	}
	_ = authzRows.Close()
	obj.Authorizations = append(obj.Authorizations, recordResources...)

	// 3. Fetch URLs (Access Methods)
	urlRows, err := db.db.QueryContext(ctx, "SELECT url, type FROM drs_object_access_method WHERE object_id = ?", id)
	if err != nil {
		return nil, err
	}
	defer urlRows.Close()

	seenAccess := make(map[string]struct{})
	for urlRows.Next() {
		var u, t string
		if err := urlRows.Scan(&u, &t); err != nil {
			return nil, err
		}
		k := t + "|" + u
		if _, ok := seenAccess[k]; ok {
			continue
		}
		seenAccess[k] = struct{}{}
		am := drs.AccessMethod{
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Type:      t,
			AccessId:  t,
			Authorizations: drs.AccessMethodAuthorizations{
				BearerAuthIssuers: recordResources,
			},
		}
		obj.AccessMethods = append(obj.AccessMethods, am)
	}

	// 4. Fetch Checksums
	hashRows, err := db.db.QueryContext(ctx, "SELECT type, checksum FROM drs_object_checksum WHERE object_id = ?", id)
	if err != nil {
		return nil, err
	}
	defer hashRows.Close()
	seenChecksum := make(map[string]struct{})
	for hashRows.Next() {
		var t, v string
		if err := hashRows.Scan(&t, &v); err != nil {
			return nil, err
		}
		key := t + "|" + v
		if _, ok := seenChecksum[key]; ok {
			continue
		}
		seenChecksum[key] = struct{}{}
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}

	// 5. RBAC Check (gen3 mode only)
	if core.IsGen3Mode(ctx) {
		userResources := core.GetUserAuthz(ctx)
		if !core.CheckAccess(recordResources, userResources) {
			return nil, fmt.Errorf("%w: access to object denied", core.ErrUnauthorized)
		}
	}

	return obj, nil
}

func (db *SqliteDB) CreateObject(ctx context.Context, obj *drs.DrsObject, authz []string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		obj.Id, obj.Size, obj.CreatedTime, obj.UpdatedTime, obj.Name, obj.Version, obj.Description,
	)
	if err != nil {
		return fmt.Errorf("failed to insert drs_object: %w", err)
	}

	// Insert URLs
	for _, am := range obj.AccessMethods {
		if am.AccessUrl.Url == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, obj.Id, am.AccessUrl.Url, am.Type)
		if err != nil {
			return fmt.Errorf("failed to insert url: %w", err)
		}
	}

	// Insert Authz
	for _, res := range authz {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES (?, ?)`, obj.Id, res)
		if err != nil {
			return fmt.Errorf("failed to insert authz: %w", err)
		}
	}

	return tx.Commit()
}

func (db *SqliteDB) RegisterObjects(ctx context.Context, objects []core.DrsObjectWithAuthz) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, obj := range objects {
		_, err = tx.ExecContext(ctx, `
			INSERT OR REPLACE INTO drs_object (id, size, created_time, updated_time, name, version, description)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			obj.Id, obj.Size, obj.CreatedTime, obj.UpdatedTime, obj.Name, obj.Version, obj.Description,
		)
		if err != nil {
			return fmt.Errorf("failed to upsert drs_object for %s: %w", obj.Id, err)
		}

		// Replace child collections atomically to avoid duplicate rows across re-registrations.
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_access_method WHERE object_id = ?`, obj.Id); err != nil {
			return fmt.Errorf("failed to clear access methods for %s: %w", obj.Id, err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_checksum WHERE object_id = ?`, obj.Id); err != nil {
			return fmt.Errorf("failed to clear checksums for %s: %w", obj.Id, err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_authz WHERE object_id = ?`, obj.Id); err != nil {
			return fmt.Errorf("failed to clear authz for %s: %w", obj.Id, err)
		}

		seenAccess := make(map[string]struct{})
		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url == "" {
				continue
			}
			key := am.Type + "|" + am.AccessUrl.Url
			if _, ok := seenAccess[key]; ok {
				continue
			}
			seenAccess[key] = struct{}{}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, obj.Id, am.AccessUrl.Url, am.Type)
			if err != nil {
				return fmt.Errorf("failed to insert url for %s: %w", obj.Id, err)
			}
		}

		seenChecksum := make(map[string]struct{})
		for _, cs := range obj.Checksums {
			key := cs.Type + "|" + cs.Checksum
			if _, ok := seenChecksum[key]; ok {
				continue
			}
			seenChecksum[key] = struct{}{}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES (?, ?, ?)`, obj.Id, cs.Type, cs.Checksum)
			if err != nil {
				return fmt.Errorf("failed to insert checksum for %s: %w", obj.Id, err)
			}
		}

		seenAuthz := make(map[string]struct{})
		for _, res := range obj.Authz {
			if _, ok := seenAuthz[res]; ok {
				continue
			}
			seenAuthz[res] = struct{}{}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES (?, ?)`, obj.Id, res)
			if err != nil {
				return fmt.Errorf("failed to insert authz for %s: %w", obj.Id, err)
			}
		}
	}

	return tx.Commit()
}

func (db *SqliteDB) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var objects []drs.DrsObject
	for _, id := range ids {
		obj, err := db.GetObject(ctx, id)
		if err != nil {
			continue
		}
		objects = append(objects, *obj)
	}
	return objects, nil
}

func (db *SqliteDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]drs.DrsObject, error) {
	if len(checksums) == 0 {
		return nil, nil
	}
	result := make(map[string][]drs.DrsObject, len(checksums))
	for _, cs := range checksums {
		objs, err := db.GetObjectsByChecksum(ctx, cs)
		if err != nil {
			return nil, err
		}
		if len(objs) > 0 {
			result[cs] = objs
		}
	}
	return result, nil
}

func (db *SqliteDB) ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `
		SELECT DISTINCT object_id
		FROM drs_object_authz
		WHERE resource = ? OR resource LIKE ?`, resourcePrefix, resourcePrefix+"/%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (db *SqliteDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	obj, err := db.GetObject(ctx, checksum)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return []drs.DrsObject{}, nil
		}
		return nil, err
	}
	return []drs.DrsObject{*obj}, nil
}

func (db *SqliteDB) BulkDeleteObjects(ctx context.Context, ids []string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, id := range ids {
		_, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ?", id)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = ?", objectID)
	if err != nil {
		return err
	}

	for _, am := range accessMethods {
		if am.AccessUrl.Url == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, objectID, am.AccessUrl.Url, am.Type)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for objectID, methods := range updates {
		_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = ?", objectID)
		if err != nil {
			return err
		}
		for _, am := range methods {
			if am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, objectID, am.AccessUrl.Url, am.Type)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	var c core.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT bucket, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE bucket = ?`, bucket).Scan(
		&c.Bucket, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("credential not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch credential: %w", err)
	}
	return &c, nil
}

func (db *SqliteDB) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	// SQLite UPSERT syntax: INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (bucket, region, access_key, secret_key, endpoint)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (bucket) DO UPDATE SET
			region = excluded.region,
			access_key = excluded.access_key,
			secret_key = excluded.secret_key,
			endpoint = excluded.endpoint`,
		cred.Bucket, cred.Region, cred.AccessKey, cred.SecretKey, cred.Endpoint,
	)
	if err != nil {
		return fmt.Errorf("failed to save credential: %w", err)
	}
	return nil
}

func (db *SqliteDB) DeleteS3Credential(ctx context.Context, bucket string) error {
	res, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE bucket = ?", bucket)
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("credential not found")
	}
	return nil
}

func (db *SqliteDB) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT bucket, region, access_key, secret_key, endpoint FROM s3_credential")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []core.S3Credential
	for rows.Next() {
		var c core.S3Credential
		if err := rows.Scan(&c.Bucket, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint); err != nil {
			return nil, err
		}
		creds = append(creds, c)
	}
	return creds, nil
}
