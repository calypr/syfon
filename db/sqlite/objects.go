package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/db/core"
)

func (db *SqliteDB) DeleteObject(ctx context.Context, id string) error {
	if aliasResult, aliasErr := db.db.ExecContext(ctx, "DELETE FROM drs_object_alias WHERE alias_id = ?", id); aliasErr == nil {
		if rows, rowsErr := aliasResult.RowsAffected(); rowsErr == nil && rows > 0 {
			return nil
		}
	}
	result, err := db.db.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ?", id)
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

func (db *SqliteDB) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
	aliasID = strings.TrimSpace(aliasID)
	canonicalObjectID = strings.TrimSpace(canonicalObjectID)
	if aliasID == "" || canonicalObjectID == "" {
		return fmt.Errorf("alias_id and canonical object id are required")
	}
	if aliasID == canonicalObjectID {
		return nil
	}

	var exists string
	err := db.db.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = ?", canonicalObjectID).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	if err != nil {
		return err
	}

	_, err = db.db.ExecContext(ctx, `
		INSERT INTO drs_object_alias(alias_id, object_id)
		VALUES (?, ?)
		ON CONFLICT(alias_id) DO UPDATE SET object_id=excluded.object_id
	`, aliasID, canonicalObjectID)
	return err
}

func (db *SqliteDB) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	aliasID = strings.TrimSpace(aliasID)
	if aliasID == "" {
		return "", fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	var canonicalID string
	err := db.db.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = ?", aliasID).Scan(&canonicalID)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	if err != nil {
		return "", err
	}
	return canonicalID, nil
}

func (db *SqliteDB) GetObject(ctx context.Context, id string) (*core.InternalObject, error) {
	requestID := strings.TrimSpace(id)
	lookupID := requestID
	resolvedAlias := false

retryLookup:
	// 1. Fetch main record
	var r core.DrsObjectRecord
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = ?`, lookupID).Scan(
		&r.ID, &r.Size, &r.CreatedTime, &r.UpdatedTime, &r.Name, &r.Version, &r.Description,
	)
	if err == sql.ErrNoRows {
		if !resolvedAlias {
			canonicalID, aliasErr := db.ResolveObjectAlias(ctx, requestID)
			if aliasErr == nil && strings.TrimSpace(canonicalID) != "" {
				lookupID = strings.TrimSpace(canonicalID)
				resolvedAlias = true
				goto retryLookup
			}
			if aliasErr != nil && !errors.Is(aliasErr, core.ErrNotFound) {
				return nil, aliasErr
			}
		}
		return nil, fmt.Errorf("%w: object not found", core.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch record: %w", err)
	}
	objectID := r.ID
	if resolvedAlias && requestID != "" {
		objectID = requestID
	}

	obj := &core.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          objectID,
			Size:        r.Size,
			CreatedTime: r.CreatedTime,
			UpdatedTime: r.UpdatedTime,
			Version:     r.Version,
			Description: r.Description,
			Name:        r.Name,
			SelfUri:     "drs://" + objectID,
		},
	}

	// 2. Fetch Authz (record-level). Read and close before subsequent queries when
	// running with a single sqlite connection.
	authzRows, err := db.db.QueryContext(ctx, "SELECT resource FROM drs_object_authz WHERE object_id = ?", lookupID)
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
	urlRows, err := db.db.QueryContext(ctx, "SELECT url, type FROM drs_object_access_method WHERE object_id = ?", lookupID)
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
	hashRows, err := db.db.QueryContext(ctx, "SELECT type, checksum FROM drs_object_checksum WHERE object_id = ?", lookupID)
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

func (db *SqliteDB) CreateObject(ctx context.Context, obj *core.InternalObject) error {
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

	// Insert Checksums
	for _, cs := range obj.Checksums {
		if strings.TrimSpace(cs.Type) == "" || strings.TrimSpace(cs.Checksum) == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES (?, ?, ?)`, obj.Id, cs.Type, cs.Checksum)
		if err != nil {
			return fmt.Errorf("failed to insert checksum: %w", err)
		}
	}

	// Insert Authz
	for _, res := range obj.Authorizations {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, resource) VALUES (?, ?)`, obj.Id, res)
		if err != nil {
			return fmt.Errorf("failed to insert authz: %w", err)
		}
	}

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, []string{obj.Id}); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *SqliteDB) RegisterObjects(ctx context.Context, objects []core.InternalObject) error {
	if len(objects) == 0 {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ids := make([]string, 0, len(objects))
	mainArgs := make([]interface{}, 0, len(objects)*7)

	accessArgs := make([]interface{}, 0)
	checksumArgs := make([]interface{}, 0)
	authzArgs := make([]interface{}, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		mainArgs = append(mainArgs, obj.Id, obj.Size, obj.CreatedTime, obj.UpdatedTime, obj.Name, obj.Version, obj.Description)

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
			accessArgs = append(accessArgs, obj.Id, am.AccessUrl.Url, am.Type)
		}

		seenChecksum := make(map[string]struct{})
		for _, cs := range obj.Checksums {
			key := cs.Type + "|" + cs.Checksum
			if _, ok := seenChecksum[key]; ok {
				continue
			}
			seenChecksum[key] = struct{}{}
			checksumArgs = append(checksumArgs, obj.Id, cs.Type, cs.Checksum)
		}

		seenAuthz := make(map[string]struct{})
		for _, res := range obj.Authorizations {
			if _, ok := seenAuthz[res]; ok {
				continue
			}
			seenAuthz[res] = struct{}{}
			authzArgs = append(authzArgs, obj.Id, res)
		}
	}

	mainPrefix := `INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description) VALUES `
	mainSuffix := ` ON CONFLICT(id) DO UPDATE SET
		size=excluded.size,
		created_time=excluded.created_time,
		updated_time=excluded.updated_time,
		name=excluded.name,
		version=excluded.version,
		description=excluded.description`
	if err := execSQLiteBulkInsert(tx, mainPrefix, "(?, ?, ?, ?, ?, ?, ?)", 7, mainArgs, mainSuffix); err != nil {
		return fmt.Errorf("failed bulk upsert drs_object: %w", err)
	}

	if err := execSQLiteDeleteByIDs(tx, "drs_object_access_method", ids); err != nil {
		return fmt.Errorf("failed bulk clear access methods: %w", err)
	}
	if err := execSQLiteDeleteByIDs(tx, "drs_object_checksum", ids); err != nil {
		return fmt.Errorf("failed bulk clear checksums: %w", err)
	}
	if err := execSQLiteDeleteByIDs(tx, "drs_object_authz", ids); err != nil {
		return fmt.Errorf("failed bulk clear authz: %w", err)
	}

	if len(accessArgs) > 0 {
		if err := execSQLiteBulkInsert(
			tx,
			"INSERT INTO drs_object_access_method (object_id, url, type) VALUES ",
			"(?, ?, ?)",
			3,
			accessArgs,
			"",
		); err != nil {
			return fmt.Errorf("failed bulk insert access methods: %w", err)
		}
	}
	if len(checksumArgs) > 0 {
		if err := execSQLiteBulkInsert(
			tx,
			"INSERT INTO drs_object_checksum (object_id, type, checksum) VALUES ",
			"(?, ?, ?)",
			3,
			checksumArgs,
			"",
		); err != nil {
			return fmt.Errorf("failed bulk insert checksums: %w", err)
		}
	}
	if len(authzArgs) > 0 {
		if err := execSQLiteBulkInsert(
			tx,
			"INSERT INTO drs_object_authz (object_id, resource) VALUES ",
			"(?, ?)",
			2,
			authzArgs,
			"",
		); err != nil {
			return fmt.Errorf("failed bulk insert authz: %w", err)
		}
	}

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *SqliteDB) GetBulkObjects(ctx context.Context, ids []string) ([]core.InternalObject, error) {
	if len(ids) == 0 {
		return nil, nil
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

func (db *SqliteDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]core.InternalObject, error) {
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

func (db *SqliteDB) ListObjectIDsByResourcePrefix(ctx context.Context, resourcePrefix string) ([]string, error) {
	if resourcePrefix == "/" {
		rows, err := db.db.QueryContext(ctx, `SELECT id FROM drs_object`)
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

func (db *SqliteDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]core.InternalObject, error) {
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

func (db *SqliteDB) BulkDeleteObjects(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	placeholders := makePlaceholders(len(ids))
	args := make([]interface{}, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	query := fmt.Sprintf("DELETE FROM drs_object WHERE id IN (%s)", placeholders)
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *SqliteDB) fetchObjectsByIDsOrChecksums(ctx context.Context, ids []string, checksums []string) (map[string]*core.InternalObject, error) {
	if len(ids) == 0 && len(checksums) == 0 {
		return map[string]*core.InternalObject{}, nil
	}

	conditions := make([]string, 0, 2)
	args := make([]interface{}, 0, len(ids)+len(checksums))
	if len(ids) > 0 {
		conditions = append(conditions, fmt.Sprintf("o.id IN (%s)", makePlaceholders(len(ids))))
		for _, id := range ids {
			args = append(args, id)
		}
	}
	if len(checksums) > 0 {
		conditions = append(conditions, fmt.Sprintf("(o.id IN (%s) OR EXISTS (SELECT 1 FROM drs_object_checksum c2 WHERE c2.object_id = o.id AND c2.checksum IN (%s)))", makePlaceholders(len(checksums)), makePlaceholders(len(checksums))))
		for _, cs := range checksums {
			args = append(args, cs)
		}
		for _, cs := range checksums {
			args = append(args, cs)
		}
	}

	query := fmt.Sprintf(`
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
		WHERE %s`, strings.Join(conditions, " OR "))

	rows, err := db.db.QueryContext(ctx, query, args...)
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

	// Apply gen3 RBAC filtering to mimic GetObject behavior.
	if core.IsGen3Mode(ctx) {
		userResources := core.GetUserAuthz(ctx)
		for id, obj := range objectsByID {
			if !core.CheckAccess(obj.Authorizations, userResources) {
				delete(objectsByID, id)
				continue
			}
		}
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
