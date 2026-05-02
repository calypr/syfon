package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func (db *SqliteDB) DeleteObject(ctx context.Context, id string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	canonicalID := strings.TrimSpace(id)
	if canonicalID == "" {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}

	if err := tx.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = ?", canonicalID).Scan(&canonicalID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	result, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ?", canonicalID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	return tx.Commit()
}

func (db *SqliteDB) DeleteObjectAlias(ctx context.Context, aliasID string) error {
	result, err := db.db.ExecContext(ctx, "DELETE FROM drs_object_alias WHERE alias_id = ?", aliasID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
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
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
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
		return "", fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	var canonicalID string
	err := db.db.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = ?", aliasID).Scan(&canonicalID)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return "", err
	}
	return canonicalID, nil
}

func (db *SqliteDB) GetObject(ctx context.Context, id string) (*models.InternalObject, error) {
	requestID := strings.TrimSpace(id)
	lookupID := requestID
	resolvedAlias := false

retryLookup:
	// 1. Fetch main record
	var r models.DrsObjectRecord
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
			if aliasErr != nil && !errors.Is(aliasErr, common.ErrNotFound) {
				return nil, aliasErr
			}
		}
		return nil, fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch record: %w", err)
	}
	objectID := r.ID
	if resolvedAlias && requestID != "" {
		objectID = requestID
	}

	obj := &models.InternalObject{
		DrsObject: drs.DrsObject{
			Id:          objectID,
			Size:        r.Size,
			CreatedTime: r.CreatedTime,
			UpdatedTime: common.Ptr(r.UpdatedTime),
			Version:     common.Ptr(r.Version),
			Description: common.Ptr(r.Description),
			Name:        common.Ptr(r.Name),
			SelfUri:     "drs://" + objectID,
		},
	}

	// 2. Fetch storage access methods.
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
		if obj.AccessMethods == nil {
			obj.AccessMethods = &[]drs.AccessMethod{}
		}
		am := drs.AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: u},
			Type:     drs.AccessMethodType(t),
			AccessId: &t,
		}
		*obj.AccessMethods = append(*obj.AccessMethods, am)
	}
	controlled, err := db.controlledAccessForObject(ctx, lookupID)
	if err != nil {
		return nil, err
	}
	if len(controlled) > 0 {
		obj.ControlledAccess = &controlled
		obj.Authorizations = sycommon.ControlledAccessToAuthzMap(controlled)
	}

	// 3. Fetch Checksums
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

	return obj, nil
}

func (db *SqliteDB) CreateObject(ctx context.Context, obj *models.InternalObject) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.StringVal(obj.Name), common.StringVal(obj.Version), common.StringVal(obj.Description),
	)
	if err != nil {
		return fmt.Errorf("failed to insert drs_object: %w", err)
	}

	if err := insertControlledAccessTx(ctx, tx, obj.Id, objectAccessResources(obj)); err != nil {
		return err
	}

	// Insert storage access methods.
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, obj.Id, am.AccessUrl.Url, am.Type)
			if err != nil {
				return fmt.Errorf("failed to insert access method: %w", err)
			}
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

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, []string{obj.Id}); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *SqliteDB) RegisterObjects(ctx context.Context, objects []models.InternalObject) error {
	if len(objects) == 0 {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ids := make([]string, 0, len(objects))
	mainCap, err := safeSliceCapacity(len(objects), len(objects), len(objects), len(objects), len(objects), len(objects), len(objects))
	if err != nil {
		return err
	}
	mainArgs := make([]interface{}, 0, mainCap)

	accessArgs := make([]interface{}, 0)
	controlledArgs := make([]interface{}, 0)
	checksumArgs := make([]interface{}, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		mainArgs = append(mainArgs, obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.StringVal(obj.Name), common.StringVal(obj.Version), common.StringVal(obj.Description))

		seenAccess := make(map[string]struct{})
		for _, resource := range objectAccessResources(&obj) {
			controlledArgs = append(controlledArgs, obj.Id, resource)
		}
		if obj.AccessMethods != nil {
			for _, am := range *obj.AccessMethods {
				if am.AccessUrl == nil || am.AccessUrl.Url == "" {
					continue
				}
				key := string(am.Type) + "|" + am.AccessUrl.Url
				if _, ok := seenAccess[key]; ok {
					continue
				}
				seenAccess[key] = struct{}{}
				accessArgs = append(accessArgs, obj.Id, am.AccessUrl.Url, am.Type)
			}
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
	if err := execSQLiteDeleteByIDs(tx, "drs_object_controlled_access", ids); err != nil {
		return fmt.Errorf("failed bulk clear controlled access: %w", err)
	}
	if err := execSQLiteDeleteByIDs(tx, "drs_object_checksum", ids); err != nil {
		return fmt.Errorf("failed bulk clear checksums: %w", err)
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
	if len(controlledArgs) > 0 {
		if err := execSQLiteBulkInsert(
			tx,
			"INSERT INTO drs_object_controlled_access (object_id, resource) VALUES ",
			"(?, ?)",
			2,
			controlledArgs,
			"",
		); err != nil {
			return fmt.Errorf("failed bulk insert controlled access: %w", err)
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
	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *SqliteDB) GetBulkObjects(ctx context.Context, ids []string) ([]models.InternalObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, ids, nil)
	if err != nil {
		return nil, err
	}
	objects := make([]models.InternalObject, 0, len(ids))
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

func (db *SqliteDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]models.InternalObject, error) {
	if len(checksums) == 0 {
		return nil, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, nil, checksums)
	if err != nil {
		return nil, err
	}
	index := make(map[string][]models.InternalObject, len(objectsByID)*2)
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
	result := make(map[string][]models.InternalObject, len(checksums))
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

func (db *SqliteDB) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		rows, err := db.db.QueryContext(ctx, `SELECT id FROM drs_object ORDER BY id`)
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

	var (
		rows *sql.Rows
		err  error
	)
	if project != "" {
		resource, err := sycommon.ResourcePath(organization, project)
		if err != nil {
			return nil, err
		}
		rows, err = db.db.QueryContext(ctx, `
			SELECT DISTINCT ca.object_id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
			WHERE ca.resource = ?
			ORDER BY ca.object_id`, resource)
	} else {
		resource, err := sycommon.ResourcePath(organization, "")
		if err != nil {
			return nil, err
		}
		rows, err = db.db.QueryContext(ctx, `
			SELECT DISTINCT ca.object_id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
			WHERE ca.resource = ?
			ORDER BY ca.object_id`, resource)
	}
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

func (db *SqliteDB) ListObjectIDsByResources(ctx context.Context, resources []string, includeUnscoped bool) ([]string, error) {
	resources = sycommon.NormalizeAccessResources(resources)
	if len(resources) == 0 && !includeUnscoped {
		return []string{}, nil
	}

	args := make([]any, 0, len(resources))
	parts := make([]string, 0, 2)
	if len(resources) > 0 {
		placeholders := make([]string, 0, len(resources))
		for _, resource := range resources {
			args = append(args, resource)
			placeholders = append(placeholders, "?")
		}
		parts = append(parts, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id AND ca.resource IN (`+strings.Join(placeholders, ",")+`)
		)`)
	}
	if includeUnscoped {
		parts = append(parts, `NOT EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id
		)`)
	}

	rows, err := db.db.QueryContext(ctx, `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE `+strings.Join(parts, " OR ")+`
		ORDER BY o.id`, args...)
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

func (db *SqliteDB) ListObjectIDsPageByScope(ctx context.Context, organization, project, startAfter string, limit, offset int) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	startAfter = strings.TrimSpace(startAfter)
	if limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	args := make([]any, 0, 4)
	conditions := make([]string, 0, 2)
	baseQuery := `SELECT id FROM drs_object`
	orderBy := ` ORDER BY id`
	objectIDExpr := "id"

	if organization != "" {
		resource, err := sycommon.ResourcePath(organization, project)
		if err != nil {
			return nil, err
		}
		args = append(args, resource)
		baseQuery = `
			SELECT DISTINCT ca.object_id AS id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
		`
		objectIDExpr = "ca.object_id"
		conditions = append(conditions, "ca.resource = ?")
		orderBy = ` ORDER BY ca.object_id`
	}
	if startAfter != "" {
		args = append(args, startAfter)
		conditions = append(conditions, objectIDExpr+" > ?")
	}
	query := baseQuery
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += orderBy + ` LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *SqliteDB) ListObjectIDsPageByResources(ctx context.Context, resources []string, includeUnscoped bool, startAfter string, limit, offset int) ([]string, error) {
	resources = sycommon.NormalizeAccessResources(resources)
	startAfter = strings.TrimSpace(startAfter)
	if limit <= 0 || (len(resources) == 0 && !includeUnscoped) {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	args := make([]any, 0, len(resources)+3)
	parts := make([]string, 0, 2)
	if len(resources) > 0 {
		placeholders := make([]string, 0, len(resources))
		for _, resource := range resources {
			args = append(args, resource)
			placeholders = append(placeholders, "?")
		}
		parts = append(parts, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id AND ca.resource IN (`+strings.Join(placeholders, ",")+`)
		)`)
	}
	if includeUnscoped {
		parts = append(parts, `NOT EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id
		)`)
	}

	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE ((` + strings.Join(parts, " OR ") + `))
	`
	if startAfter != "" {
		query += ` AND o.id > ?`
		args = append(args, startAfter)
	}
	query += ` ORDER BY o.id LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *SqliteDB) ListObjectIDsPageByChecksum(ctx context.Context, checksum, checksumType, organization, project, startAfter string, limit, offset int, resources []string, includeUnscoped, restrictToResources bool) ([]string, error) {
	checksum = strings.TrimSpace(checksum)
	checksumType = strings.TrimSpace(checksumType)
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	startAfter = strings.TrimSpace(startAfter)
	if checksum == "" || limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	args := make([]any, 0, 8)
	conditions := make([]string, 0, 4)

	args = append(args, checksum)
	if checksumType == "" {
		conditions = append(conditions, `(o.id = ? OR EXISTS (
			SELECT 1
			FROM drs_object_checksum c2
			WHERE c2.object_id = o.id AND c2.checksum = ?
		))`)
		args = append(args, checksum)
	} else {
		conditions = append(conditions, `EXISTS (
			SELECT 1
			FROM drs_object_checksum c2
			WHERE c2.object_id = o.id AND c2.checksum = ? AND c2.type = ?
		)`)
		args = append(args, checksumType)
	}
	if organization != "" {
		resource, err := sycommon.ResourcePath(organization, project)
		if err != nil {
			return nil, err
		}
		args = append(args, resource)
		conditions = append(conditions, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND ca_scope.resource = ?
		)`)
	}
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return []string{}, nil
		}
		parts := make([]string, 0, 2)
		if len(resources) > 0 {
			placeholders := make([]string, 0, len(resources))
			for _, resource := range resources {
				args = append(args, resource)
				placeholders = append(placeholders, "?")
			}
			parts = append(parts, `EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource IN (`+strings.Join(placeholders, ",")+`)
			)`)
		}
		if includeUnscoped {
			parts = append(parts, `NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)`)
		}
		conditions = append(conditions, `(`+strings.Join(parts, " OR ")+`)`)
	}
	if startAfter != "" {
		args = append(args, startAfter)
		conditions = append(conditions, `o.id > ?`)
	}

	query := `
		SELECT o.id
		FROM drs_object o
		WHERE ` + strings.Join(conditions, ` AND `) + `
		ORDER BY o.id LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *SqliteDB) ListObjectIDsByScopeAndResources(ctx context.Context, organization, project string, resources []string, restrictToResources bool) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		if !restrictToResources {
			rows, err := db.db.QueryContext(ctx, `SELECT id FROM drs_object ORDER BY id`)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			return scanObjectIDs(rows)
		}
		return db.ListObjectIDsByResources(ctx, resources, false)
	}

	scopeResource, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return nil, err
	}
	args := make([]any, 0, len(resources)+1)
	args = append(args, scopeResource)
	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND ca_scope.resource = ?
		)`
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 {
			return []string{}, nil
		}
		placeholders := make([]string, 0, len(resources))
		for _, resource := range resources {
			args = append(args, resource)
			placeholders = append(placeholders, "?")
		}
		query += `
		AND EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_auth
			WHERE ca_auth.object_id = o.id AND ca_auth.resource IN (` + strings.Join(placeholders, ",") + `)
		)`
	}
	query += ` ORDER BY o.id`
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *SqliteDB) ListObjectIDsByChecksumsAndResources(ctx context.Context, checksums []string, resources []string, includeUnscoped, restrictToResources bool) (map[string][]string, error) {
	normalized := make([]string, 0, len(checksums))
	for _, checksum := range checksums {
		if trimmed := strings.TrimSpace(checksum); trimmed != "" {
			normalized = append(normalized, trimmed)
		}
	}
	checksums = normalized
	if len(checksums) == 0 {
		return map[string][]string{}, nil
	}

	args := make([]any, 0, len(checksums)*2+len(resources))
	idPlaceholders := make([]string, 0, len(checksums))
	checksumPlaceholders := make([]string, 0, len(checksums))
	for _, checksum := range checksums {
		args = append(args, checksum)
		idPlaceholders = append(idPlaceholders, "?")
	}
	for _, checksum := range checksums {
		args = append(args, checksum)
		checksumPlaceholders = append(checksumPlaceholders, "?")
	}
	query := `
		WITH matched AS (
			SELECT id AS object_id, id AS match_key
			FROM drs_object
			WHERE id IN (` + strings.Join(idPlaceholders, ",") + `)
			UNION
			SELECT c.object_id, c.checksum AS match_key
			FROM drs_object_checksum c
			WHERE c.checksum IN (` + strings.Join(checksumPlaceholders, ",") + `)
		)
		SELECT m.match_key, m.object_id
		FROM matched m
		INNER JOIN drs_object o ON o.id = m.object_id`
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return map[string][]string{}, nil
		}
		parts := make([]string, 0, 2)
		if len(resources) > 0 {
			placeholders := make([]string, 0, len(resources))
			for _, resource := range resources {
				args = append(args, resource)
				placeholders = append(placeholders, "?")
			}
			parts = append(parts, `EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource IN (`+strings.Join(placeholders, ",")+`)
			)`)
		}
		if includeUnscoped {
			parts = append(parts, `NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)`)
		}
		query += ` WHERE (` + strings.Join(parts, " OR ") + `)`
	}
	query += ` ORDER BY m.match_key, m.object_id`
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChecksumMatchRows(rows)
}

func (db *SqliteDB) ListBucketVisibilityRows(ctx context.Context, resources []string, includeUnscoped, restrictToResources bool) ([]models.BucketVisibilityRow, error) {
	args := make([]any, 0, len(resources))
	query := `
		SELECT DISTINCT am.url, am.type, COALESCE(ca.resource, '')
		FROM drs_object o
		INNER JOIN drs_object_access_method am ON am.object_id = o.id
		LEFT JOIN drs_object_controlled_access ca ON ca.object_id = o.id`
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return []models.BucketVisibilityRow{}, nil
		}
		parts := make([]string, 0, 2)
		if len(resources) > 0 {
			placeholders := make([]string, 0, len(resources))
			for _, resource := range resources {
				args = append(args, resource)
				placeholders = append(placeholders, "?")
			}
			parts = append(parts, `EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource IN (`+strings.Join(placeholders, ",")+`)
			)`)
		}
		if includeUnscoped {
			parts = append(parts, `NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)`)
		}
		query += ` WHERE (` + strings.Join(parts, " OR ") + `)`
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.BucketVisibilityRow, 0)
	for rows.Next() {
		var row models.BucketVisibilityRow
		if err := rows.Scan(&row.AccessURL, &row.AccessType, &row.Resource); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *SqliteDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]models.InternalObject, error) {
	checksum = strings.TrimSpace(checksum)
	if checksum == "" {
		return []models.InternalObject{}, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, nil, []string{checksum})
	if err != nil {
		return nil, err
	}
	if len(objectsByID) == 0 {
		return []models.InternalObject{}, nil
	}
	out := make([]models.InternalObject, 0, len(objectsByID))
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

func scanObjectIDs(rows *sql.Rows) ([]string, error) {
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

func scanChecksumMatchRows(rows *sql.Rows) (map[string][]string, error) {
	out := make(map[string][]string)
	for rows.Next() {
		var checksum, objectID string
		if err := rows.Scan(&checksum, &objectID); err != nil {
			return nil, err
		}
		ids := out[checksum]
		if len(ids) > 0 && ids[len(ids)-1] == objectID {
			continue
		}
		out[checksum] = append(ids, objectID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (db *SqliteDB) fetchObjectsByIDsOrChecksums(ctx context.Context, ids []string, checksums []string) (map[string]*models.InternalObject, error) {
	if len(ids) == 0 && len(checksums) == 0 {
		return map[string]*models.InternalObject{}, nil
	}

	conditions := make([]string, 0, 2)
	capArgs, err := safeSliceCapacity(len(ids), len(checksums), len(checksums))
	if err != nil {
		return nil, err
	}
	args := make([]interface{}, 0, capArgs)
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
			cs.checksum
		FROM drs_object o
		LEFT JOIN drs_object_access_method am ON am.object_id = o.id
		LEFT JOIN drs_object_checksum cs ON cs.object_id = o.id
		WHERE %s`, strings.Join(conditions, " OR "))

	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bulk objects: %w", err)
	}
	defer rows.Close()

	objectsByID := make(map[string]*models.InternalObject)
	seenAccess := make(map[string]map[string]struct{})
	seenChecksum := make(map[string]map[string]struct{})

	for rows.Next() {
		var (
			id, name, version, description string
			size                           int64
			createdTime, updatedTime       time.Time
			accessURL, accessType          sql.NullString
			checksumType, sumVal           sql.NullString
		)
		if err := rows.Scan(
			&id, &size, &createdTime, &updatedTime, &name, &version, &description,
			&accessURL, &accessType, &checksumType, &sumVal,
		); err != nil {
			return nil, err
		}

		obj, ok := objectsByID[id]
		if !ok {
			obj = &models.InternalObject{
				DrsObject: drs.DrsObject{
					Id:          id,
					Size:        size,
					CreatedTime: createdTime,
					UpdatedTime: common.Ptr(updatedTime),
					Name:        common.Ptr(name),
					Version:     common.Ptr(version),
					Description: common.Ptr(description),
					SelfUri:     "drs://" + id,
				},
			}
			objectsByID[id] = obj
			seenAccess[id] = make(map[string]struct{})
			seenChecksum[id] = make(map[string]struct{})
		}

		if accessURL.Valid && accessType.Valid {
			key := accessType.String + "|" + accessURL.String
			if _, exists := seenAccess[id][key]; !exists {
				seenAccess[id][key] = struct{}{}
				if obj.DrsObject.AccessMethods == nil {
					obj.DrsObject.AccessMethods = &[]drs.AccessMethod{}
				}
				t := accessType.String
				*obj.DrsObject.AccessMethods = append(*obj.DrsObject.AccessMethods, drs.AccessMethod{
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: accessURL.String},
					Type:     drs.AccessMethodType(accessType.String),
					AccessId: &t,
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
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := db.attachControlledAccess(ctx, objectsByID); err != nil {
		return nil, err
	}

	return objectsByID, nil
}

func objectAccessResources(obj *models.InternalObject) []string {
	if obj == nil {
		return nil
	}
	if obj.ControlledAccess != nil {
		return sycommon.NormalizeAccessResources(*obj.ControlledAccess)
	}
	return sycommon.AuthzMapToList(obj.Authorizations)
}

func insertControlledAccessTx(ctx context.Context, tx *sql.Tx, objectID string, resources []string) error {
	for _, resource := range sycommon.NormalizeAccessResources(resources) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO drs_object_controlled_access (object_id, resource) VALUES (?, ?)`, objectID, resource); err != nil {
			return fmt.Errorf("failed to insert controlled access: %w", err)
		}
	}
	return nil
}

func (db *SqliteDB) controlledAccessForObject(ctx context.Context, objectID string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `SELECT resource FROM drs_object_controlled_access WHERE object_id = ? ORDER BY resource`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var resources []string
	for rows.Next() {
		var resource string
		if err := rows.Scan(&resource); err != nil {
			return nil, err
		}
		resources = append(resources, resource)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sycommon.NormalizeAccessResources(resources), nil
}

func (db *SqliteDB) attachControlledAccess(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	ids := make([]any, 0, len(objectsByID))
	placeholders := make([]string, 0, len(objectsByID))
	for id := range objectsByID {
		ids = append(ids, id)
		placeholders = append(placeholders, "?")
	}
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, resource
		FROM drs_object_controlled_access
		WHERE object_id IN (`+strings.Join(placeholders, ",")+`)
		ORDER BY object_id, resource`, ids...)
	if err != nil {
		return err
	}
	defer rows.Close()

	byObject := make(map[string][]string, len(objectsByID))
	for rows.Next() {
		var objectID, resource string
		if err := rows.Scan(&objectID, &resource); err != nil {
			return err
		}
		byObject[objectID] = append(byObject[objectID], resource)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for id, resources := range byObject {
		obj, ok := objectsByID[id]
		if !ok {
			continue
		}
		controlled := sycommon.NormalizeAccessResources(resources)
		if len(controlled) == 0 {
			continue
		}
		obj.ControlledAccess = &controlled
		obj.Authorizations = sycommon.ControlledAccessToAuthzMap(controlled)
	}
	return nil
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
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
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
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
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
