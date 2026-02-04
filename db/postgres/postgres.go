package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	_ "github.com/lib/pq"
)

// PostgresDB implements DatabaseInterface
var _ core.DatabaseInterface = (*PostgresDB)(nil)

type PostgresDB struct {
	db *sql.DB
}

func NewPostgresDB(dsn string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	return &PostgresDB{db: db}, nil
}

func (db *PostgresDB) GetServiceInfo(ctx context.Context) (*drs.Service, error) {
	// Static info for now, or fetch from DB if stored there
	return &drs.Service{
		Id:          "drs-service-calypr",
		Name:        "Calypr DRS Server",
		Type:        drs.ServiceType{Group: "org.ga4gh", Artifact: "drs", Version: "1.2.0"},
		Description: "Calypr-backed DRS server",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Environment: "prod",
		Version:     "1.0.0",
	}, nil
}

func (db *PostgresDB) DeleteObject(ctx context.Context, id string) error {
	result, err := db.db.ExecContext(ctx, "DELETE FROM drs_object WHERE id = $1", id)
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

func (db *PostgresDB) GetObject(ctx context.Context, id string) (*drs.DrsObject, error) {
	// 1. Fetch main record
	var r core.DrsObjectRecord
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`, id).Scan(
		&r.ID, &r.Size, &r.CreatedTime, &r.UpdatedTime, &r.Name, &r.Version, &r.Description,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("object not found")
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

	// 2. Fetch URLs (Access Methods)
	urlRows, err := db.db.QueryContext(ctx, "SELECT url, type FROM drs_object_access_method WHERE object_id = $1", id)
	if err != nil {
		return nil, err
	}
	defer urlRows.Close()
	for urlRows.Next() {
		var u, t string
		if err := urlRows.Scan(&u, &t); err != nil {
			return nil, err
		}
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Type:      t,
			AccessId:  u,
		})
	}

	// 3. Fetch Checksums
	hashRows, err := db.db.QueryContext(ctx, "SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1", id)
	if err != nil {
		return nil, err
	}
	defer hashRows.Close()
	for hashRows.Next() {
		var t, v string
		if err := hashRows.Scan(&t, &v); err != nil {
			return nil, err
		}
		obj.Checksums = append(obj.Checksums, drs.Checksum{Type: t, Checksum: v})
	}

	// 4. RBAC Check
	authzRows, err := db.db.QueryContext(ctx, "SELECT resource FROM drs_object_authz WHERE object_id = $1", id)
	if err != nil {
		return nil, err
	}
	defer authzRows.Close()
	var recordResources []string
	for authzRows.Next() {
		var res string
		if err := authzRows.Scan(&res); err != nil {
			return nil, err
		}
		recordResources = append(recordResources, res)
	}

	userResources := core.GetUserAuthz(ctx)
	if !core.CheckAccess(recordResources, userResources) {
		return nil, fmt.Errorf("unauthorized access to object")
	}

	return obj, nil
}

func (db *PostgresDB) CreateObject(ctx context.Context, obj *drs.DrsObject) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
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
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, obj.Id, am.AccessUrl.Url, am.Type)
		if err != nil {
			return fmt.Errorf("failed to insert url: %w", err)
		}
	}

	// Insert Checksums
	for _, cs := range obj.Checksums {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES ($1, $2, $3)`, obj.Id, cs.Type, cs.Checksum)
		if err != nil {
			return fmt.Errorf("failed to insert checksum: %w", err)
		}
	}

	return tx.Commit()
}

func (db *PostgresDB) RegisterObjects(ctx context.Context, objects []drs.DrsObject) error {
	// Simple loop-based transaction for bulk registration
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, obj := range objects {
		// Use the same logic as CreateObject but within this transaction
		_, err = tx.ExecContext(ctx, `
			INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			obj.Id, obj.Size, obj.CreatedTime, obj.UpdatedTime, obj.Name, obj.Version, obj.Description,
		)
		if err != nil {
			return fmt.Errorf("failed to insert drs_object for %s: %w", obj.Id, err)
		}

		for _, am := range obj.AccessMethods {
			if am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, obj.Id, am.AccessUrl.Url, am.Type)
			if err != nil {
				return fmt.Errorf("failed to insert url for %s: %w", obj.Id, err)
			}
		}

		for _, cs := range obj.Checksums {
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES ($1, $2, $3)`, obj.Id, cs.Type, cs.Checksum)
			if err != nil {
				return fmt.Errorf("failed to insert checksum for %s: %w", obj.Id, err)
			}
		}
	}

	return tx.Commit()
}

func (db *PostgresDB) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	// Naive implementation for now: loop and fetch.
	// Optimally, use WHERE did IN (...)
	var objects []drs.DrsObject
	for _, id := range ids {
		obj, err := db.GetObject(ctx, id)
		if err != nil {
			// Skip objects that are not found or unauthorized
			continue
		}
		objects = append(objects, *obj)
	}
	return objects, nil
}

func (db *PostgresDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	// Identify IDs first
	rows, err := db.db.QueryContext(ctx, "SELECT object_id FROM drs_object_checksum WHERE checksum = $1", checksum)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}

	// Now fetch objects
	return db.GetBulkObjects(ctx, ids)
}

func (db *PostgresDB) BulkDeleteObjects(ctx context.Context, ids []string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, id := range ids {
		_, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = $1", id)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *PostgresDB) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = $1", objectID)
	if err != nil {
		return err
	}

	for _, am := range accessMethods {
		if am.AccessUrl.Url == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, objectID, am.AccessUrl.Url, am.Type)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *PostgresDB) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for objectID, methods := range updates {
		_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = $1", objectID)
		if err != nil {
			return err
		}
		for _, am := range methods {
			if am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, objectID, am.AccessUrl.Url, am.Type)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// S3 Credential Management

func (db *PostgresDB) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	var c core.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT bucket, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE bucket = $1`, bucket).Scan(
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

func (db *PostgresDB) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (bucket, region, access_key, secret_key, endpoint)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (bucket) DO UPDATE SET
			region = EXCLUDED.region,
			access_key = EXCLUDED.access_key,
			secret_key = EXCLUDED.secret_key,
			endpoint = EXCLUDED.endpoint`,
		cred.Bucket, cred.Region, cred.AccessKey, cred.SecretKey, cred.Endpoint,
	)
	if err != nil {
		return fmt.Errorf("failed to save credential: %w", err)
	}
	return nil
}

func (db *PostgresDB) DeleteS3Credential(ctx context.Context, bucket string) error {
	res, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE bucket = $1", bucket)
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

func (db *PostgresDB) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
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
