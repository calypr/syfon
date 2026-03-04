package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/lib/pq"
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

	// 2. Fetch URLs (Access Methods)
	urlRows, err := db.db.QueryContext(ctx, "SELECT url, type FROM drs_object_access_method WHERE object_id = $1", id)
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
		key := t + "|" + u
		if _, ok := seenAccess[key]; ok {
			continue
		}
		seenAccess[key] = struct{}{}
		obj.AccessMethods = append(obj.AccessMethods, drs.AccessMethod{
			AccessUrl: drs.AccessMethodAccessUrl{Url: u},
			Type:      t,
			AccessId:  t,
		})
	}

	// 3. Fetch Checksums
	hashRows, err := db.db.QueryContext(ctx, "SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1", id)
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

	// 4. Fetch object-level authz resources.
	authzRows, err := db.db.QueryContext(ctx, "SELECT resource FROM drs_object_authz WHERE object_id = $1", id)
	if err != nil {
		return nil, err
	}
	defer authzRows.Close()
	seenAuthz := make(map[string]struct{})
	for authzRows.Next() {
		var res string
		if err := authzRows.Scan(&res); err != nil {
			return nil, err
		}
		if _, ok := seenAuthz[res]; ok {
			continue
		}
		seenAuthz[res] = struct{}{}
		obj.Authorizations = append(obj.Authorizations, res)
	}
	for i := range obj.AccessMethods {
		obj.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
			BearerAuthIssuers: obj.Authorizations,
		}
	}

	// 5. RBAC Check (gen3 mode only)
	if !core.IsGen3Mode(ctx) {
		return obj, nil
	}

	// Optimized in SQL for gen3 mode.
	userResources := core.GetUserAuthz(ctx)

	var count int
	err = db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM drs_object o
		WHERE o.id = $1 AND (
			NOT EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id)
			OR EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id AND a.resource = ANY($2))
		)`, id, pq.Array(userResources)).Scan(&count)

	if err != nil {
		return nil, fmt.Errorf("authorization check failed: %w", err)
	}
	if count == 0 {
		return nil, fmt.Errorf("%w: access to object denied", core.ErrUnauthorized)
	}

	return obj, nil
}

func (db *PostgresDB) CreateObject(ctx context.Context, obj *drs.DrsObject, authz []string) error {
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

	// Insert Authz
	for _, res := range authz {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES ($1, $2)`, obj.Id, res)
		if err != nil {
			return fmt.Errorf("failed to insert authz: %w", err)
		}
	}

	return tx.Commit()
}

func (db *PostgresDB) RegisterObjects(ctx context.Context, objects []core.DrsObjectWithAuthz) error {
	// Simple loop-based transaction for bulk registration
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, obj := range objects {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO UPDATE SET
				size = EXCLUDED.size,
				created_time = EXCLUDED.created_time,
				updated_time = EXCLUDED.updated_time,
				name = EXCLUDED.name,
				version = EXCLUDED.version,
				description = EXCLUDED.description`,
			obj.Id, obj.Size, obj.CreatedTime, obj.UpdatedTime, obj.Name, obj.Version, obj.Description,
		)
		if err != nil {
			return fmt.Errorf("failed to insert drs_object for %s: %w", obj.Id, err)
		}

		// Replace child collections atomically to avoid duplicate rows across re-registrations.
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_access_method WHERE object_id = $1`, obj.Id); err != nil {
			return fmt.Errorf("failed to clear access methods for %s: %w", obj.Id, err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_checksum WHERE object_id = $1`, obj.Id); err != nil {
			return fmt.Errorf("failed to clear checksums for %s: %w", obj.Id, err)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM drs_object_authz WHERE object_id = $1`, obj.Id); err != nil {
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
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, obj.Id, am.AccessUrl.Url, am.Type)
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
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES ($1, $2, $3)`, obj.Id, cs.Type, cs.Checksum)
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
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES ($1, $2)`, obj.Id, res)
			if err != nil {
				return fmt.Errorf("failed to insert authz for %s: %w", obj.Id, err)
			}
		}
	}

	return tx.Commit()
}

func (db *PostgresDB) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	if len(ids) == 0 {
		return []drs.DrsObject{}, nil
	}

	userResources := core.GetUserAuthz(ctx)

	// Fetch authorized IDs
	rows, err := db.db.QueryContext(ctx, `
		SELECT o.id FROM drs_object o
		WHERE o.id = ANY($1) AND (
			NOT EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id)
			OR EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id AND a.resource = ANY($2))
		)`, pq.Array(ids), pq.Array(userResources))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch authorized IDs: %w", err)
	}
	defer rows.Close()

	var authorizedIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		authorizedIDs = append(authorizedIDs, id)
	}

	var objects []drs.DrsObject
	for _, id := range authorizedIDs {
		// Use GetObject but skip redundant auth check since we already filtered?
		// Actually GetObject still does auth check. We can keep it or refactor.
		// For simplicity and correctness, we call GetObject.
		// Since we already filtered IDs, GetObject should succeed.
		obj, err := db.GetObject(ctx, id)
		if err != nil {
			continue
		}
		objects = append(objects, *obj)
	}
	return objects, nil
}

func (db *PostgresDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error) {
	obj, err := db.GetObject(ctx, checksum)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return []drs.DrsObject{}, nil
		}
		return nil, err
	}
	return []drs.DrsObject{*obj}, nil
}

func (db *PostgresDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]drs.DrsObject, error) {
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

func (db *PostgresDB) ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `
		SELECT DISTINCT object_id
		FROM drs_object_authz
		WHERE resource = $1 OR resource LIKE $2`, resourcePrefix, resourcePrefix+"/%")
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
