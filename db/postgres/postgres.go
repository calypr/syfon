package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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
	pg := &PostgresDB{db: db}
	if err := pg.ensureBucketScopeSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureLFSPendingSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensureObjectUsageSchema(); err != nil {
		return nil, err
	}
	if err := pg.ensurePendingObjectUsageSchema(); err != nil {
		return nil, err
	}
	return pg, nil
}

func (db *PostgresDB) ensureBucketScopeSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS bucket_scope (
			organization TEXT NOT NULL,
			project_id TEXT NOT NULL,
			bucket TEXT NOT NULL,
			path_prefix TEXT NULL,
			PRIMARY KEY (organization, project_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_bucket_scope_bucket ON bucket_scope(bucket)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize bucket scope schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureLFSPendingSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS lfs_pending_metadata (
			oid TEXT PRIMARY KEY,
			candidate_json JSONB NOT NULL,
			created_time TIMESTAMPTZ NOT NULL,
			expires_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_expires ON lfs_pending_metadata(expires_time)`,
		`CREATE INDEX IF NOT EXISTS idx_lfs_pending_metadata_created ON lfs_pending_metadata(created_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize lfs pending metadata schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensureObjectUsageSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS object_usage (
			object_id TEXT PRIMARY KEY REFERENCES drs_object(id) ON DELETE CASCADE,
			upload_count BIGINT NOT NULL DEFAULT 0,
			download_count BIGINT NOT NULL DEFAULT 0,
			last_upload_time TIMESTAMPTZ NULL,
			last_download_time TIMESTAMPTZ NULL,
			updated_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_download_time ON object_usage(last_download_time)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_last_upload_time ON object_usage(last_upload_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object usage schema: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) ensurePendingObjectUsageSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS object_usage_event (
			id BIGSERIAL PRIMARY KEY,
			object_id TEXT NOT NULL,
			event_type TEXT NOT NULL CHECK (event_type IN ('upload','download')),
			event_time TIMESTAMPTZ NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_object_id ON object_usage_event(object_id)`,
		`CREATE INDEX IF NOT EXISTS idx_object_usage_event_event_time ON object_usage_event(event_time)`,
	}
	for _, q := range queries {
		if _, err := db.db.Exec(q); err != nil {
			return fmt.Errorf("failed to initialize object usage event schema: %w", err)
		}
	}
	return nil
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
		return fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	return nil
}

func (db *PostgresDB) GetObject(ctx context.Context, id string) (*core.InternalObject, error) {
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

	obj := &core.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          r.ID,
			Size:        r.Size,
			CreatedTime: r.CreatedTime,
			UpdatedTime: r.UpdatedTime,
			Version:     r.Version,
			Description: r.Description,
			Name:        r.Name,
			SelfUri:     "drs://" + r.ID,
		},
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

func (db *PostgresDB) CreateObject(ctx context.Context, obj *core.InternalObject) error {
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

	// Insert checksums
	for _, cs := range obj.Checksums {
		if strings.TrimSpace(cs.Type) == "" || strings.TrimSpace(cs.Checksum) == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES ($1, $2, $3)`, obj.Id, cs.Type, cs.Checksum)
		if err != nil {
			return fmt.Errorf("failed to insert checksum: %w", err)
		}
	}

	// Insert Authz
	for _, res := range obj.Authorizations {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES ($1, $2)`, obj.Id, res)
		if err != nil {
			return fmt.Errorf("failed to insert authz: %w", err)
		}
	}

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, []string{obj.Id}); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) RegisterObjects(ctx context.Context, objects []core.InternalObject) error {
	if len(objects) == 0 {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ids := make([]string, 0, len(objects))
	sizes := make([]int64, 0, len(objects))
	createdTimes := make([]time.Time, 0, len(objects))
	updatedTimes := make([]time.Time, 0, len(objects))
	names := make([]string, 0, len(objects))
	versions := make([]string, 0, len(objects))
	descriptions := make([]string, 0, len(objects))

	accessObjectIDs := make([]string, 0)
	accessURLs := make([]string, 0)
	accessTypes := make([]string, 0)

	checksumObjectIDs := make([]string, 0)
	checksumTypes := make([]string, 0)
	checksumValues := make([]string, 0)

	authzObjectIDs := make([]string, 0)
	authzResources := make([]string, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		sizes = append(sizes, obj.Size)
		createdTimes = append(createdTimes, obj.CreatedTime)
		updatedTimes = append(updatedTimes, obj.UpdatedTime)
		names = append(names, obj.Name)
		versions = append(versions, obj.Version)
		descriptions = append(descriptions, obj.Description)

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
			accessObjectIDs = append(accessObjectIDs, obj.Id)
			accessURLs = append(accessURLs, am.AccessUrl.Url)
			accessTypes = append(accessTypes, am.Type)
		}

		seenChecksum := make(map[string]struct{})
		for _, cs := range obj.Checksums {
			key := cs.Type + "|" + cs.Checksum
			if _, ok := seenChecksum[key]; ok {
				continue
			}
			seenChecksum[key] = struct{}{}
			checksumObjectIDs = append(checksumObjectIDs, obj.Id)
			checksumTypes = append(checksumTypes, cs.Type)
			checksumValues = append(checksumValues, cs.Checksum)
		}

		seenAuthz := make(map[string]struct{})
		for _, res := range obj.Authorizations {
			if _, ok := seenAuthz[res]; ok {
				continue
			}
			seenAuthz[res] = struct{}{}
			authzObjectIDs = append(authzObjectIDs, obj.Id)
			authzResources = append(authzResources, res)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		SELECT * FROM UNNEST($1::text[], $2::bigint[], $3::timestamp[], $4::timestamp[], $5::text[], $6::text[], $7::text[])
		ON CONFLICT (id) DO UPDATE SET
			size = EXCLUDED.size,
			created_time = EXCLUDED.created_time,
			updated_time = EXCLUDED.updated_time,
			name = EXCLUDED.name,
			version = EXCLUDED.version,
			description = EXCLUDED.description`,
		pq.Array(ids), pq.Array(sizes), pq.Array(createdTimes), pq.Array(updatedTimes),
		pq.Array(names), pq.Array(versions), pq.Array(descriptions),
	); err != nil {
		return fmt.Errorf("failed bulk upsert drs_object: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_access_method WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear access methods: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_checksum WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear checksums: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_authz WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear authz: %w", err)
	}

	if len(accessObjectIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drs_object_access_method (object_id, url, type)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])`,
			pq.Array(accessObjectIDs), pq.Array(accessURLs), pq.Array(accessTypes),
		); err != nil {
			return fmt.Errorf("failed bulk insert access methods: %w", err)
		}
	}

	if len(checksumObjectIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drs_object_checksum (object_id, type, checksum)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])`,
			pq.Array(checksumObjectIDs), pq.Array(checksumTypes), pq.Array(checksumValues),
		); err != nil {
			return fmt.Errorf("failed bulk insert checksums: %w", err)
		}
	}

	if len(authzObjectIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drs_object_authz (object_id, resource)
			SELECT * FROM UNNEST($1::text[], $2::text[])`,
			pq.Array(authzObjectIDs), pq.Array(authzResources),
		); err != nil {
			return fmt.Errorf("failed bulk insert authz: %w", err)
		}
	}

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) flushObjectUsageEventsForIDsTx(ctx context.Context, tx *sql.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO object_usage (object_id, upload_count, download_count, last_upload_time, last_download_time, updated_time)
		SELECT e.object_id,
			COALESCE(SUM(CASE WHEN e.event_type = 'upload' THEN 1 ELSE 0 END), 0) AS upload_count,
			COALESCE(SUM(CASE WHEN e.event_type = 'download' THEN 1 ELSE 0 END), 0) AS download_count,
			MAX(CASE WHEN e.event_type = 'upload' THEN e.event_time END) AS last_upload_time,
			MAX(CASE WHEN e.event_type = 'download' THEN e.event_time END) AS last_download_time,
			$2
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
		WHERE e.object_id = ANY($1)
		GROUP BY e.object_id
		ON CONFLICT (object_id) DO UPDATE SET
			upload_count = object_usage.upload_count + EXCLUDED.upload_count,
			download_count = object_usage.download_count + EXCLUDED.download_count,
			last_upload_time = CASE
				WHEN EXCLUDED.last_upload_time IS NULL THEN object_usage.last_upload_time
				WHEN object_usage.last_upload_time IS NULL THEN EXCLUDED.last_upload_time
				WHEN EXCLUDED.last_upload_time > object_usage.last_upload_time THEN EXCLUDED.last_upload_time
				ELSE object_usage.last_upload_time
			END,
			last_download_time = CASE
				WHEN EXCLUDED.last_download_time IS NULL THEN object_usage.last_download_time
				WHEN object_usage.last_download_time IS NULL THEN EXCLUDED.last_download_time
				WHEN EXCLUDED.last_download_time > object_usage.last_download_time THEN EXCLUDED.last_download_time
				ELSE object_usage.last_download_time
			END,
			updated_time = EXCLUDED.updated_time
	`, pq.Array(ids), now); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		DELETE FROM object_usage_event e
		USING drs_object o
		WHERE e.object_id = o.id AND e.object_id = ANY($1)
	`, pq.Array(ids))
	return err
}

func (db *PostgresDB) GetBulkObjects(ctx context.Context, ids []string) ([]core.InternalObject, error) {
	if len(ids) == 0 {
		return []core.InternalObject{}, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, ids, nil)
	if err != nil {
		return nil, err
	}
	objects := make([]core.InternalObject, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if obj, ok := objectsByID[id]; ok {
			objects = append(objects, *obj)
		}
	}
	return objects, nil
}

func (db *PostgresDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]core.InternalObject, error) {
	checksum = strings.TrimSpace(checksum)
	if checksum == "" {
		return []core.InternalObject{}, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, nil, []string{checksum})
	if err != nil {
		return nil, err
	}
	if len(objectsByID) == 0 {
		return []core.InternalObject{}, nil
	}
	out := make([]core.InternalObject, 0, len(objectsByID))
	for _, obj := range objectsByID {
		out = append(out, *obj)
	}
	return uniqueObjectsByID(out), nil
}

func (db *PostgresDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]core.InternalObject, error) {
	if len(checksums) == 0 {
		return nil, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, nil, checksums)
	if err != nil {
		return nil, err
	}
	index := make(map[string][]core.InternalObject, len(objectsByID)*2)
	for _, obj := range objectsByID {
		index[obj.Id] = append(index[obj.Id], *obj)
		for _, cs := range obj.Checksums {
			value := strings.TrimSpace(cs.Checksum)
			if value == "" {
				continue
			}
			index[value] = append(index[value], *obj)
		}
	}
	result := make(map[string][]core.InternalObject, len(checksums))
	for _, cs := range checksums {
		normalized := strings.TrimSpace(cs)
		if normalized == "" {
			continue
		}
		if objs := index[normalized]; len(objs) > 0 {
			result[normalized] = uniqueObjectsByID(objs)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (db *PostgresDB) BulkDeleteObjects(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ANY($1)", pq.Array(ids)); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *PostgresDB) fetchObjectsByIDsOrChecksums(ctx context.Context, ids []string, checksums []string) (map[string]*core.InternalObject, error) {
	if len(ids) == 0 && len(checksums) == 0 {
		return map[string]*core.InternalObject{}, nil
	}

	userResources := core.GetUserAuthz(ctx)
	rows, err := db.db.QueryContext(ctx, `
		SELECT
			o.id,
			o.size,
			o.created_time,
			o.updated_time,
			o.name,
			o.version,
			o.description,
			am.url,
			am.type,
			cs.type,
			cs.checksum,
			oa.resource
		FROM drs_object o
		LEFT JOIN drs_object_access_method am ON am.object_id = o.id
		LEFT JOIN drs_object_checksum cs ON cs.object_id = o.id
		LEFT JOIN drs_object_authz oa ON oa.object_id = o.id
		WHERE (
			(COALESCE(array_length($1::text[], 1), 0) > 0 AND o.id = ANY($1))
			OR
			(COALESCE(array_length($2::text[], 1), 0) > 0 AND (
				o.id = ANY($2)
				OR EXISTS (
					SELECT 1
					FROM drs_object_checksum c2
					WHERE c2.object_id = o.id AND c2.checksum = ANY($2)
				)
			))
		)
		AND (
			NOT EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id)
			OR EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id AND a.resource = ANY($3))
		)`,
		pq.Array(ids), pq.Array(checksums), pq.Array(userResources),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bulk objects: %w", err)
	}
	defer rows.Close()

	objectsByID := make(map[string]*core.InternalObject)
	seenAccess := make(map[string]map[string]struct{})
	seenChecksum := make(map[string]map[string]struct{})
	seenAuthz := make(map[string]map[string]struct{})

	for rows.Next() {
		var (
			id, name, version, description              string
			size                                        int64
			createdTime, updatedTime                    time.Time
			accessURL, accessType, checksumType, sumVal sql.NullString
			authzResource                               sql.NullString
		)
		if err := rows.Scan(
			&id, &size, &createdTime, &updatedTime, &name, &version, &description,
			&accessURL, &accessType, &checksumType, &sumVal, &authzResource,
		); err != nil {
			return nil, err
		}

		obj, ok := objectsByID[id]
		if !ok {
			obj = &core.InternalObject{
				DrsObject: drs.DrsObject{
					Id:          id,
					Size:        size,
					CreatedTime: createdTime,
					UpdatedTime: updatedTime,
					Name:        name,
					Version:     version,
					Description: description,
					SelfUri:     "drs://" + id,
				},
			}
			objectsByID[id] = obj
			seenAccess[id] = make(map[string]struct{})
			seenChecksum[id] = make(map[string]struct{})
			seenAuthz[id] = make(map[string]struct{})
		}

		if accessURL.Valid && accessType.Valid {
			key := accessType.String + "|" + accessURL.String
			if _, exists := seenAccess[id][key]; !exists {
				seenAccess[id][key] = struct{}{}
				obj.DrsObject.AccessMethods = append(obj.DrsObject.AccessMethods, drs.AccessMethod{
					AccessUrl: drs.AccessMethodAccessUrl{Url: accessURL.String},
					Type:      accessType.String,
					AccessId:  accessType.String,
				})
			}
		}

		if checksumType.Valid && sumVal.Valid {
			key := checksumType.String + "|" + sumVal.String
			if _, exists := seenChecksum[id][key]; !exists {
				seenChecksum[id][key] = struct{}{}
				obj.DrsObject.Checksums = append(obj.DrsObject.Checksums, drs.Checksum{Type: checksumType.String, Checksum: sumVal.String})
			}
		}

		if authzResource.Valid && strings.TrimSpace(authzResource.String) != "" {
			res := authzResource.String
			if _, exists := seenAuthz[id][res]; !exists {
				seenAuthz[id][res] = struct{}{}
				obj.Authorizations = append(obj.Authorizations, res)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, obj := range objectsByID {
		for i := range obj.DrsObject.AccessMethods {
			obj.DrsObject.AccessMethods[i].Authorizations = drs.AccessMethodAuthorizations{
				BearerAuthIssuers: obj.Authorizations,
			}
		}
	}

	return objectsByID, nil
}

func uniqueObjectsByID(objs []core.InternalObject) []core.InternalObject {
	seen := make(map[string]struct{}, len(objs))
	out := make([]core.InternalObject, 0, len(objs))
	for _, o := range objs {
		if _, ok := seen[o.Id]; ok {
			continue
		}
		seen[o.Id] = struct{}{}
		out = append(out, o)
	}
	return out
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
	_, _ = db.db.ExecContext(ctx, "DELETE FROM bucket_scope WHERE bucket = $1", bucket)
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

func (db *PostgresDB) CreateBucketScope(ctx context.Context, scope *core.BucketScope) error {
	if scope == nil {
		return fmt.Errorf("scope is required")
	}
	org := strings.TrimSpace(scope.Organization)
	project := strings.TrimSpace(scope.ProjectID)
	bucket := strings.TrimSpace(scope.Bucket)
	prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
	if org == "" || project == "" || bucket == "" {
		return fmt.Errorf("organization, project_id and bucket are required")
	}

	existing, err := db.GetBucketScope(ctx, org, project)
	if err != nil && !errors.Is(err, core.ErrNotFound) {
		return err
	}
	if err == nil && existing != nil {
		if strings.EqualFold(strings.TrimSpace(existing.Bucket), bucket) && strings.Trim(strings.TrimSpace(existing.PathPrefix), "/") == prefix {
			return nil
		}
		return fmt.Errorf("%w: scope already assigned to bucket=%s prefix=%s", core.ErrConflict, existing.Bucket, existing.PathPrefix)
	}

	_, err = db.db.ExecContext(ctx, `
		INSERT INTO bucket_scope (organization, project_id, bucket, path_prefix)
		VALUES ($1, $2, $3, $4)
	`, org, project, bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to create bucket scope: %w", err)
	}
	return nil
}

func (db *PostgresDB) GetBucketScope(ctx context.Context, organization, projectID string) (*core.BucketScope, error) {
	var s core.BucketScope
	err := db.db.QueryRowContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
	`, strings.TrimSpace(organization), strings.TrimSpace(projectID)).Scan(
		&s.Organization, &s.ProjectID, &s.Bucket, &s.PathPrefix,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: bucket scope not found", core.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket scope: %w", err)
	}
	return &s, nil
}

func (db *PostgresDB) ListBucketScopes(ctx context.Context) ([]core.BucketScope, error) {
	rows, err := db.db.QueryContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []core.BucketScope
	for rows.Next() {
		var s core.BucketScope
		if err := rows.Scan(&s.Organization, &s.ProjectID, &s.Bucket, &s.PathPrefix); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func (db *PostgresDB) SavePendingLFSMeta(ctx context.Context, entries []core.PendingLFSMeta) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= NOW()`); err != nil {
		return fmt.Errorf("failed to prune expired pending metadata: %w", err)
	}

	for _, e := range entries {
		raw, err := json.Marshal(e.Candidate)
		if err != nil {
			return fmt.Errorf("failed to marshal pending candidate for oid %s: %w", e.OID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO lfs_pending_metadata (oid, candidate_json, created_time, expires_time)
			VALUES ($1, $2::jsonb, $3, $4)
			ON CONFLICT (oid) DO UPDATE SET
				candidate_json = EXCLUDED.candidate_json,
				created_time = EXCLUDED.created_time,
				expires_time = EXCLUDED.expires_time
		`, e.OID, string(raw), e.CreatedAt.UTC(), e.ExpiresAt.UTC()); err != nil {
			return fmt.Errorf("failed to save pending metadata for oid %s: %w", e.OID, err)
		}
	}
	return tx.Commit()
}

func (db *PostgresDB) GetPendingLFSMeta(ctx context.Context, oid string) (*core.PendingLFSMeta, error) {
	if _, err := db.db.ExecContext(ctx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= NOW()`); err != nil {
		return nil, fmt.Errorf("failed to prune expired pending metadata: %w", err)
	}

	var (
		raw       string
		createdAt time.Time
		expiresAt time.Time
	)
	if err := db.db.QueryRowContext(ctx, `
		SELECT candidate_json::text, created_time, expires_time
		FROM lfs_pending_metadata
		WHERE oid = $1 AND expires_time > NOW()
	`, oid).Scan(&raw, &createdAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
		}
		return nil, fmt.Errorf("failed to load pending metadata for oid %s: %w", oid, err)
	}

	var c drs.DrsObjectCandidate
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, fmt.Errorf("failed to parse pending metadata candidate for oid %s: %w", oid, err)
	}

	return &core.PendingLFSMeta{
		OID:       oid,
		Candidate: c,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}, nil
}

func (db *PostgresDB) PopPendingLFSMeta(ctx context.Context, oid string) (*core.PendingLFSMeta, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= NOW()`); err != nil {
		return nil, fmt.Errorf("failed to prune expired pending metadata: %w", err)
	}

	var (
		raw       string
		createdAt time.Time
		expiresAt time.Time
	)
	if err := tx.QueryRowContext(ctx, `
		SELECT candidate_json::text, created_time, expires_time
		FROM lfs_pending_metadata
		WHERE oid = $1 AND expires_time > NOW()
		FOR UPDATE
	`, oid).Scan(&raw, &createdAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: pending metadata not found", core.ErrNotFound)
		}
		return nil, fmt.Errorf("failed to load pending metadata for oid %s: %w", oid, err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM lfs_pending_metadata WHERE oid = $1`, oid); err != nil {
		return nil, fmt.Errorf("failed to consume pending metadata for oid %s: %w", oid, err)
	}

	var c drs.DrsObjectCandidate
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, fmt.Errorf("failed to parse pending metadata candidate for oid %s: %w", oid, err)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &core.PendingLFSMeta{
		OID:       oid,
		Candidate: c,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	}, nil
}

func (db *PostgresDB) RecordFileUpload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES ($1, 'upload', $2)
	`, objectID, now)
	return err
}

func (db *PostgresDB) RecordFileDownload(ctx context.Context, objectID string) error {
	now := time.Now().UTC()
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO object_usage_event (object_id, event_type, event_time)
		VALUES ($1, 'download', $2)
	`, objectID, now)
	return err
}

func (db *PostgresDB) GetFileUsage(ctx context.Context, objectID string) (*core.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	var usage core.FileUsage
	var lastUpload sql.NullTime
	var lastDownload sql.NullTime
	err := db.db.QueryRowContext(ctx, `
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
		WHERE o.id = $1
	`, objectID).Scan(
		&usage.ObjectID,
		&usage.Name,
		&usage.Size,
		&usage.UploadCount,
		&usage.DownloadCount,
		&lastUpload,
		&lastDownload,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: file usage not found", core.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	if lastUpload.Valid {
		t := lastUpload.Time
		usage.LastUploadTime = &t
	}
	if lastDownload.Valid {
		t := lastDownload.Time
		usage.LastDownloadTime = &t
	}
	usage.LastAccessTime = latestUsageTime(usage.LastUploadTime, usage.LastDownloadTime)
	return &usage, nil
}

func (db *PostgresDB) ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]core.FileUsage, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > 1000 {
		limit = 1000
	}
	if offset < 0 {
		offset = 0
	}

	query := `
		SELECT o.id, o.name, o.size,
			COALESCE(u.upload_count, 0),
			COALESCE(u.download_count, 0),
			u.last_upload_time,
			u.last_download_time
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
	`
	args := []any{}
	argPos := 1
	if inactiveSince != nil {
		query += fmt.Sprintf(" WHERE u.last_download_time IS NULL OR u.last_download_time < $%d", argPos)
		args = append(args, inactiveSince.UTC())
		argPos++
	}
	query += fmt.Sprintf(" ORDER BY COALESCE(u.last_download_time, TIMESTAMPTZ '1970-01-01T00:00:00Z') ASC, o.id ASC LIMIT $%d OFFSET $%d", argPos, argPos+1)
	args = append(args, limit, offset)

	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]core.FileUsage, 0, limit)
	for rows.Next() {
		var usage core.FileUsage
		var lastUpload sql.NullTime
		var lastDownload sql.NullTime
		if err := rows.Scan(
			&usage.ObjectID,
			&usage.Name,
			&usage.Size,
			&usage.UploadCount,
			&usage.DownloadCount,
			&lastUpload,
			&lastDownload,
		); err != nil {
			return nil, err
		}
		if lastUpload.Valid {
			t := lastUpload.Time
			usage.LastUploadTime = &t
		}
		if lastDownload.Valid {
			t := lastDownload.Time
			usage.LastDownloadTime = &t
		}
		usage.LastAccessTime = latestUsageTime(usage.LastUploadTime, usage.LastDownloadTime)
		out = append(out, usage)
	}
	return out, nil
}

func (db *PostgresDB) GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (core.FileUsageSummary, error) {
	if err := db.flushObjectUsageEvents(ctx); err != nil {
		return core.FileUsageSummary{}, err
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -730)
	if inactiveSince != nil {
		cutoff = inactiveSince.UTC()
	}
	var summary core.FileUsageSummary
	if err := db.db.QueryRowContext(ctx, `
		SELECT
			COUNT(o.id) AS total_files,
			COALESCE(SUM(COALESCE(u.upload_count, 0)), 0) AS total_uploads,
			COALESCE(SUM(COALESCE(u.download_count, 0)), 0) AS total_downloads,
			COALESCE(SUM(CASE WHEN u.last_download_time IS NULL OR u.last_download_time < $1 THEN 1 ELSE 0 END), 0) AS inactive_files
		FROM drs_object o
		LEFT JOIN object_usage u ON u.object_id = o.id
	`, cutoff).Scan(
		&summary.TotalFiles,
		&summary.TotalUploads,
		&summary.TotalDownloads,
		&summary.InactiveFileCount,
	); err != nil {
		return core.FileUsageSummary{}, err
	}
	return summary, nil
}

func (db *PostgresDB) flushObjectUsageEvents(ctx context.Context) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ids, err := db.existingObjectIDsWithEvents(ctx, tx)
	if err != nil {
		return err
	}
	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *PostgresDB) existingObjectIDsWithEvents(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT e.object_id
		FROM object_usage_event e
		JOIN drs_object o ON o.id = e.object_id
	`)
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

func latestUsageTime(ts ...*time.Time) *time.Time {
	var latest *time.Time
	for _, t := range ts {
		if t == nil {
			continue
		}
		if latest == nil || t.After(*latest) {
			copyT := *t
			latest = &copyT
		}
	}
	return latest
}
