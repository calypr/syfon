package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/db/core"
	"github.com/lib/pq"
)

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
