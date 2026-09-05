package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

func (db *SqliteDB) DeleteObject(ctx context.Context, id string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	requestedID := strings.TrimSpace(id)
	if requestedID == "" {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	canonicalID, found, err := sqliteObjectIDTx(ctx, tx, requestedID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err := sqliteEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
		return err
	}
	if err := sqliteRequireContentMethodTx(ctx, tx, canonicalID, "delete"); err != nil {
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
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "DELETE FROM drs_object_alias WHERE alias_id = ?", aliasID)
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

func (db *SqliteDB) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
	aliasID = strings.TrimSpace(aliasID)
	canonicalObjectID = strings.TrimSpace(canonicalObjectID)
	if aliasID == "" || canonicalObjectID == "" {
		return fmt.Errorf("alias_id and canonical object id are required")
	}
	if aliasID == canonicalObjectID {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var exists string
	err = tx.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = ?", canonicalObjectID).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return err
	}
	if err := sqliteEnsureNoLegacyDuplicateTx(ctx, tx, canonicalObjectID); err != nil {
		return err
	}
	if err := sqliteRequireContentMethodTx(ctx, tx, canonicalObjectID, "update"); err != nil {
		return err
	}
	var physicalAlias string
	physicalErr := tx.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = ?", aliasID).Scan(&physicalAlias)
	if physicalErr == nil {
		return fmt.Errorf("%w: alias %q is already a physical object", common.ErrConflict, aliasID)
	}
	if physicalErr != sql.ErrNoRows {
		return physicalErr
	}

	var aliasTarget string
	aliasErr := tx.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = ?", aliasID).Scan(&aliasTarget)
	if aliasErr == nil && aliasTarget != canonicalObjectID {
		return fmt.Errorf("%w: alias %q already points to %q", common.ErrConflict, aliasID, aliasTarget)
	}
	if aliasErr != nil && aliasErr != sql.ErrNoRows {
		return aliasErr
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object_alias(alias_id, object_id)
		VALUES (?, ?)
		ON CONFLICT(alias_id) DO NOTHING
	`, aliasID, canonicalObjectID)
	if err != nil {
		return err
	}
	return tx.Commit()
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
	var name, version, description sql.NullString
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = ?`, lookupID).Scan(
		&r.ID, &r.Size, &r.CreatedTime, &r.UpdatedTime, &name, &version, &description,
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
	r.Name = strings.TrimSpace(name.String)
	r.Version = version.String
	r.Description = description.String
	nameAliases, err := db.nameAliasesForObject(ctx, lookupID)
	if err != nil {
		return nil, err
	}
	objectID := r.ID

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
		NameAliases: nameAliases,
		Properties:  map[string]interface{}{},
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
			AccessId: common.Ptr(common.AccessMethodID(t, u)),
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
	obj.PublicRead, obj.PublicReadPolicyKnown, err = db.publicReadForObject(ctx, lookupID, len(controlled) == 0)
	if err != nil {
		return nil, err
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

func (db *SqliteDB) createObjectLegacy(ctx context.Context, obj *models.InternalObject) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.CleanToBasename(common.StringVal(obj.Name)), common.StringVal(obj.Version), common.StringVal(obj.Description),
	)
	if err != nil {
		return fmt.Errorf("failed to insert drs_object: %w", err)
	}

	if err := insertControlledAccessTx(ctx, tx, obj.Id, objectAccessResources(obj)); err != nil {
		return err
	}
	for _, alias := range normalizeObjectNameAliases(obj) {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_name_alias (object_id, name_alias) VALUES (?, ?)`, obj.Id, alias)
		if err != nil {
			return fmt.Errorf("failed to insert name alias: %w", err)
		}
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

func (db *SqliteDB) registerObjectsLegacy(ctx context.Context, objects []models.InternalObject) error {
	if len(objects) == 0 {
		return nil
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ids := make([]string, 0, len(objects))
	mainCap, err := safeSliceCapacity(len(objects), len(objects), len(objects), len(objects), len(objects), len(objects), len(objects), len(objects))
	if err != nil {
		return err
	}
	mainArgs := make([]interface{}, 0, mainCap)

	accessArgs := make([]interface{}, 0)
	controlledArgs := make([]interface{}, 0)
	checksumArgs := make([]interface{}, 0)
	nameAliasArgs := make([]interface{}, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		mainArgs = append(mainArgs, obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.CleanToBasename(common.StringVal(obj.Name)), common.StringVal(obj.Version), common.StringVal(obj.Description))

		seenAccess := make(map[string]struct{})
		for _, resource := range objectAccessResources(&obj) {
			controlledArgs = append(controlledArgs, obj.Id, resource)
		}
		for _, alias := range normalizeObjectNameAliases(&obj) {
			nameAliasArgs = append(nameAliasArgs, obj.Id, alias)
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
	if err := execSQLiteDeleteByIDs(tx, "drs_object_name_alias", ids); err != nil {
		return fmt.Errorf("failed bulk clear name aliases: %w", err)
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
	if len(nameAliasArgs) > 0 {
		if err := execSQLiteBulkInsert(
			tx,
			"INSERT INTO drs_object_name_alias (object_id, name_alias) VALUES ",
			"(?, ?)",
			2,
			nameAliasArgs,
			"",
		); err != nil {
			return fmt.Errorf("failed bulk insert name aliases: %w", err)
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
		obj, ok := objectsByID[id]
		if !ok {
			resolved, resolveErr := db.ResolveObjectAlias(ctx, id)
			if resolveErr == nil {
				obj, ok = objectsByID[resolved]
				if !ok {
					obj, resolveErr = db.GetObject(ctx, resolved)
					ok = resolveErr == nil
				}
			} else if !errors.Is(resolveErr, common.ErrNotFound) {
				return nil, resolveErr
			}
			if resolveErr != nil && !errors.Is(resolveErr, common.ErrNotFound) {
				return nil, resolveErr
			}
		}
		if !ok || obj == nil {
			continue
		}
		if _, already := seen[obj.Id]; already {
			continue
		}
		seen[obj.Id] = struct{}{}
		objects = append(objects, *obj)
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
			if common.NormalizeChecksumType(cs.Type) == "sha256" {
				if normalized, ok := common.NormalizeSHA256Query(value); ok {
					index[normalized] = append(index[normalized], *obj)
				}
			}
		}
	}
	result := make(map[string][]models.InternalObject, len(checksums))
	for _, cs := range checksums {
		requested := strings.TrimSpace(cs)
		if requested == "" {
			continue
		}
		lookup := requested
		if normalized, ok := common.NormalizeSHA256Query(requested); ok {
			lookup = normalized
		}
		if objs := index[lookup]; len(objs) > 0 {
			result[requested] = uniqueObjectsByID(objs)
		}
	}
	return result, nil
}

func (db *SqliteDB) ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" || len(checksums) == 0 {
		return map[string][]string{}, nil
	}
	resource, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return nil, err
	}
	normalized := make([]string, 0, len(checksums))
	seenChecksums := make(map[string]struct{}, len(checksums))
	for _, checksum := range checksums {
		value := normalizeChecksumLookup(checksum)
		if value == "" {
			continue
		}
		if _, exists := seenChecksums[value]; exists {
			continue
		}
		seenChecksums[value] = struct{}{}
		normalized = append(normalized, value)
	}
	if len(normalized) == 0 {
		return map[string][]string{}, nil
	}
	// Keep the checksum predicate below within SQLite's bound-parameter limit.
	// The two fixed parameters are the project resource and checksum type.
	maxChecksums := sqliteMaxParams - 2
	if len(normalized) > maxChecksums {
		out := make(map[string][]string, len(normalized))
		for start := 0; start < len(normalized); start += maxChecksums {
			end := start + maxChecksums
			if end > len(normalized) {
				end = len(normalized)
			}
			part, err := db.ListScopedObjectIDsByChecksums(ctx, organization, project, normalized[start:end])
			if err != nil {
				return nil, err
			}
			for checksum, ids := range part {
				out[checksum] = append(out[checksum], ids...)
			}
		}
		return out, nil
	}
	args := make([]any, 0, len(normalized)+2)
	args = append(args, resource, "sha256")
	placeholders := makePlaceholders(len(normalized))
	for _, checksum := range normalized {
		args = append(args, checksum)
	}
	rows, err := db.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT replace(lower(trim(c.checksum)), 'sha256:', ''), c.object_id
		FROM drs_object_checksum c
		INNER JOIN drs_object_controlled_access ca ON ca.object_id = c.object_id
		WHERE ca.resource = ? AND replace(lower(trim(c.type)), '-', '') = ?
		  AND replace(lower(trim(c.checksum)), 'sha256:', '') IN (%s)
		ORDER BY 1, 2`, placeholders), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]string, len(normalized))
	for _, checksum := range normalized {
		out[checksum] = []string{}
	}
	seen := make(map[string]map[string]struct{}, len(normalized))
	for rows.Next() {
		var checksum string
		var objectID string
		if err := rows.Scan(&checksum, &objectID); err != nil {
			return nil, err
		}
		checksum = strings.TrimSpace(checksum)
		objectID = strings.TrimSpace(objectID)
		if checksum == "" || objectID == "" {
			continue
		}
		if seen[checksum] == nil {
			seen[checksum] = make(map[string]struct{})
		}
		if _, ok := seen[checksum][objectID]; ok {
			continue
		}
		seen[checksum][objectID] = struct{}{}
		out[checksum] = append(out[checksum], objectID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
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
			WHERE replace(lower(trim(c.checksum)), 'sha256:', '') IN (` + strings.Join(checksumPlaceholders, ",") + `)
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

func (db *SqliteDB) ListObjectIDsPageByURL(ctx context.Context, objectURL, organization, project, startAfter string, limit, offset int, resources []string, includeUnscoped, restrictToResources bool) ([]string, error) {
	objectURL = strings.TrimSpace(objectURL)
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	startAfter = strings.TrimSpace(startAfter)
	if objectURL == "" || limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	args := []any{objectURL}
	conditions := []string{"am.url = ?"}
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
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}
	if startAfter != "" {
		args = append(args, startAfter)
		conditions = append(conditions, "o.id > ?")
	}

	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		INNER JOIN drs_object_access_method am ON am.object_id = o.id
		WHERE ` + strings.Join(conditions, " AND ") + `
		ORDER BY o.id
		LIMIT ? OFFSET ?`
	args = append(args, limit, offset)
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
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
	canonicalIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, rawID := range ids {
		canonicalID, found, resolveErr := sqliteObjectIDTx(ctx, tx, strings.TrimSpace(rawID))
		if resolveErr != nil {
			return resolveErr
		}
		if !found {
			continue
		}
		if strings.TrimSpace(rawID) != canonicalID {
			if err := sqliteEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
				return err
			}
		}
		if err := sqliteRequireContentMethodTx(ctx, tx, canonicalID, "delete"); err != nil {
			return err
		}
		if _, ok := seen[canonicalID]; ok {
			continue
		}
		seen[canonicalID] = struct{}{}
		canonicalIDs = append(canonicalIDs, canonicalID)
	}
	if len(canonicalIDs) == 0 {
		return tx.Commit()
	}

	placeholders := makePlaceholders(len(canonicalIDs))
	args := make([]interface{}, 0, len(canonicalIDs))
	for _, id := range canonicalIDs {
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
	shaQueries := make([]string, 0, len(checksums))
	genericQueries := make([]string, 0, len(checksums))
	for _, checksum := range checksums {
		if normalized, ok := common.NormalizeSHA256Query(checksum); ok {
			shaQueries = append(shaQueries, normalized)
		} else {
			genericQueries = append(genericQueries, strings.TrimSpace(checksum))
		}
	}
	capArgs, err := safeSliceCapacity(len(ids), len(checksums), len(shaQueries)+len(genericQueries))
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
		parts := make([]string, 0, 3)
		parts = append(parts, fmt.Sprintf("o.id IN (%s)", makePlaceholders(len(checksums))))
		for _, cs := range checksums {
			args = append(args, strings.TrimSpace(cs))
		}
		if len(shaQueries) > 0 {
			parts = append(parts, fmt.Sprintf(`EXISTS (SELECT 1 FROM drs_object_checksum c2
				WHERE c2.object_id = o.id AND replace(lower(trim(c2.type)), '-', '') = 'sha256'
				AND replace(lower(trim(c2.checksum)), 'sha256:', '') IN (%s))`, makePlaceholders(len(shaQueries))))
			for _, checksum := range shaQueries {
				args = append(args, checksum)
			}
		}
		if len(genericQueries) > 0 {
			parts = append(parts, fmt.Sprintf(`EXISTS (SELECT 1 FROM drs_object_checksum c2
				WHERE c2.object_id = o.id AND c2.checksum IN (%s))`, makePlaceholders(len(genericQueries))))
			for _, checksum := range genericQueries {
				args = append(args, checksum)
			}
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}

	query := fmt.Sprintf(`
		SELECT
			o.id,
			o.size,
			o.created_time,
			o.updated_time,
			o.name,
			o.version,
			o.description
		FROM drs_object o
		WHERE %s`, strings.Join(conditions, " OR "))

	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bulk objects: %w", err)
	}
	defer rows.Close()

	objectsByID := make(map[string]*models.InternalObject)

	for rows.Next() {
		var (
			id                       string
			name                     sql.NullString
			version, description     sql.NullString
			size                     int64
			createdTime, updatedTime time.Time
		)
		if err := rows.Scan(
			&id, &size, &createdTime, &updatedTime, &name, &version, &description,
		); err != nil {
			return nil, err
		}
		objectsByID[id] = &models.InternalObject{
			DrsObject: drs.DrsObject{
				Id:          id,
				Size:        size,
				CreatedTime: createdTime,
				UpdatedTime: common.Ptr(updatedTime),
				Name:        common.Ptr(strings.TrimSpace(name.String)),
				Version:     common.Ptr(version.String),
				Description: common.Ptr(description.String),
				SelfUri:     "drs://" + id,
			},
			Properties: map[string]interface{}{},
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(objectsByID) == 0 {
		return objectsByID, nil
	}
	if err := db.attachBulkAccessMethods(ctx, objectsByID); err != nil {
		return nil, err
	}
	if err := db.attachBulkChecksums(ctx, objectsByID); err != nil {
		return nil, err
	}
	if err := db.attachControlledAccess(ctx, objectsByID); err != nil {
		return nil, err
	}
	if err := db.attachPublicRead(ctx, objectsByID); err != nil {
		return nil, err
	}
	if err := db.attachNameAliases(ctx, objectsByID); err != nil {
		return nil, err
	}

	return objectsByID, nil
}

func (db *SqliteDB) attachBulkAccessMethods(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	ids := sortedObjectIDs(objectsByID)
	query := fmt.Sprintf(`
		SELECT object_id, url, type
		FROM drs_object_access_method
		WHERE object_id IN (%s)
		ORDER BY object_id`, makePlaceholders(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to fetch bulk object access methods: %w", err)
	}
	defer rows.Close()

	seenAccess := make(map[string]map[string]struct{}, len(objectsByID))
	for rows.Next() {
		var objectID, accessURL, accessType string
		if err := rows.Scan(&objectID, &accessURL, &accessType); err != nil {
			return err
		}
		obj := objectsByID[objectID]
		if obj == nil {
			continue
		}
		if _, ok := seenAccess[objectID]; !ok {
			seenAccess[objectID] = make(map[string]struct{})
		}
		key := accessType + "|" + accessURL
		if _, exists := seenAccess[objectID][key]; exists {
			continue
		}
		seenAccess[objectID][key] = struct{}{}
		if obj.DrsObject.AccessMethods == nil {
			obj.DrsObject.AccessMethods = &[]drs.AccessMethod{}
		}
		*obj.DrsObject.AccessMethods = append(*obj.DrsObject.AccessMethods, drs.AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: accessURL},
			Type:     drs.AccessMethodType(accessType),
			AccessId: common.Ptr(common.AccessMethodID(accessType, accessURL)),
		})
	}
	return rows.Err()
}

func (db *SqliteDB) attachBulkChecksums(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	ids := sortedObjectIDs(objectsByID)
	query := fmt.Sprintf(`
		SELECT object_id, type, checksum
		FROM drs_object_checksum
		WHERE object_id IN (%s)
		ORDER BY object_id`, makePlaceholders(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to fetch bulk object checksums: %w", err)
	}
	defer rows.Close()

	seenChecksums := make(map[string]map[string]struct{}, len(objectsByID))
	for rows.Next() {
		var objectID, checksumType, checksumValue string
		if err := rows.Scan(&objectID, &checksumType, &checksumValue); err != nil {
			return err
		}
		obj := objectsByID[objectID]
		if obj == nil {
			continue
		}
		if _, ok := seenChecksums[objectID]; !ok {
			seenChecksums[objectID] = make(map[string]struct{})
		}
		key := checksumType + "|" + checksumValue
		if _, exists := seenChecksums[objectID][key]; exists {
			continue
		}
		seenChecksums[objectID][key] = struct{}{}
		obj.DrsObject.Checksums = append(obj.DrsObject.Checksums, drs.Checksum{Type: checksumType, Checksum: checksumValue})
	}
	return rows.Err()
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

func normalizeObjectNameAliases(obj *models.InternalObject) []string {
	if obj == nil {
		return nil
	}
	return common.NormalizeNameAliases(common.StringVal(obj.Name), obj.NameAliases)
}

func sortedObjectIDs(objectsByID map[string]*models.InternalObject) []string {
	ids := make([]string, 0, len(objectsByID))
	for id := range objectsByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		normalized := strings.TrimSpace(value)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
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

func (db *SqliteDB) nameAliasesForObject(ctx context.Context, objectID string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `SELECT name_alias FROM drs_object_name_alias WHERE object_id = ? ORDER BY name_alias`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	aliases := make([]string, 0)
	for rows.Next() {
		var alias string
		if err := rows.Scan(&alias); err != nil {
			return nil, err
		}
		aliases = append(aliases, alias)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return common.NormalizeNameAliases("", aliases), nil
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

func (db *SqliteDB) attachPublicRead(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	ids := sortedObjectIDs(objectsByID)
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT object_id, public_read
		FROM drs_object_read_policy
		WHERE object_id IN (%s)`, makePlaceholders(len(ids))), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	known := make(map[string]bool, len(ids))
	for rows.Next() {
		var id string
		var public bool
		if err := rows.Scan(&id, &public); err != nil {
			return err
		}
		known[id] = public
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for id, obj := range objectsByID {
		public, ok := known[id]
		if !ok {
			public = len(objectAccessResources(obj)) == 0
		}
		obj.PublicRead = public
		obj.PublicReadPolicyKnown = ok
	}
	return nil
}

func (db *SqliteDB) attachNameAliases(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	ids := sortedObjectIDs(objectsByID)
	query := fmt.Sprintf(`
		SELECT object_id, name_alias
		FROM drs_object_name_alias
		WHERE object_id IN (%s)
		ORDER BY object_id, name_alias`, makePlaceholders(len(ids)))
	args := make([]any, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to fetch bulk object name aliases: %w", err)
	}
	defer rows.Close()

	byObject := make(map[string][]string, len(objectsByID))
	for rows.Next() {
		var objectID, alias string
		if err := rows.Scan(&objectID, &alias); err != nil {
			return err
		}
		byObject[objectID] = append(byObject[objectID], alias)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for objectID, aliases := range byObject {
		obj := objectsByID[objectID]
		if obj == nil {
			continue
		}
		obj.NameAliases = common.NormalizeNameAliases(common.StringVal(obj.Name), aliases)
	}
	return nil
}

func (db *SqliteDB) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	canonicalID, found, err := sqliteObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err := sqliteRequireContentMethodTx(ctx, tx, canonicalID, "update"); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = ?", canonicalID)
	if err != nil {
		return err
	}

	for _, am := range accessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, canonicalID, am.AccessUrl.Url, am.Type)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *SqliteDB) RemoveObjectControlledAccess(ctx context.Context, objectID, resource string) error {
	normalized := sycommon.NormalizeAccessResources([]string{resource})
	if len(normalized) == 0 {
		return fmt.Errorf("resource is required")
	}
	resource = normalized[0]
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	canonicalID, found, err := sqliteObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
	if err != nil {
		return err
	}
	if !found {
		return common.ErrNotFound
	}
	if err := sqliteEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
		return err
	}
	if !authz.HasMethodAccess(ctx, "update", []string{resource}) {
		return common.ErrUnauthorized
	}

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM drs_object_controlled_access WHERE object_id = ? AND resource = ?`, canonicalID, resource).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return common.ErrNotFound
	}
	currentResources, err := sqliteResourcesTx(ctx, tx, canonicalID)
	if err != nil {
		return err
	}
	publicRead, err := sqlitePublicReadTx(ctx, tx, canonicalID, len(currentResources) == 0)
	if err != nil {
		return err
	}
	if err := setPublicReadTx(ctx, tx, canonicalID, publicRead); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_controlled_access WHERE object_id = ? AND resource = ?`, canonicalID, resource); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *SqliteDB) RemoveObjectControlledAccessBulk(ctx context.Context, objectIDs []string, resource string) (int, error) {
	if len(objectIDs) == 0 {
		return 0, nil
	}
	normalized := sycommon.NormalizeAccessResources([]string{resource})
	if len(normalized) == 0 {
		return 0, fmt.Errorf("resource is required")
	}
	resource = normalized[0]
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	orgWide := !strings.Contains(resource, "/project/")
	if !orgWide && !authz.HasMethodAccess(ctx, "delete", []string{resource}) {
		return 0, common.ErrUnauthorized
	}
	seen := make(map[string]struct{}, len(objectIDs))
	removed := 0
	for _, rawID := range objectIDs {
		canonicalID, found, resolveErr := sqliteObjectIDTx(ctx, tx, strings.TrimSpace(rawID))
		if resolveErr != nil {
			return 0, resolveErr
		}
		if !found {
			continue
		}
		if _, ok := seen[canonicalID]; ok {
			continue
		}
		seen[canonicalID] = struct{}{}
		if err := sqliteEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
			return 0, err
		}
		currentResources, err := sqliteResourcesTx(ctx, tx, canonicalID)
		if err != nil {
			return 0, err
		}
		objectRemoved := 0
		for _, currentResource := range currentResources {
			if currentResource != resource && (!orgWide || !strings.HasPrefix(currentResource, resource+"/project/")) {
				continue
			}
			if !authz.HasMethodAccess(ctx, "delete", []string{currentResource}) {
				continue
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_controlled_access WHERE object_id = ? AND resource = ?`, canonicalID, currentResource); err != nil {
				return 0, err
			}
			removed++
			objectRemoved++
		}
		if objectRemoved == 0 {
			continue
		}
		currentResources, err = sqliteResourcesTx(ctx, tx, canonicalID)
		if err != nil {
			return 0, err
		}
		publicRead, err := sqlitePublicReadTx(ctx, tx, canonicalID, len(currentResources) == 0)
		if err != nil {
			return 0, err
		}
		if err := setPublicReadTx(ctx, tx, canonicalID, publicRead); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

func (db *SqliteDB) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for objectID, methods := range updates {
		canonicalID, found, resolveErr := sqliteObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
		if resolveErr != nil {
			return resolveErr
		}
		if !found {
			return common.ErrNotFound
		}
		if err := sqliteRequireContentMethodTx(ctx, tx, canonicalID, "update"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = ?", canonicalID)
		if err != nil {
			return err
		}
		for _, am := range methods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES (?, ?, ?)`, canonicalID, am.AccessUrl.Url, am.Type)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
