package postgres

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
	"github.com/lib/pq"
)

func (db *PostgresDB) DeleteObject(ctx context.Context, id string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}

	requestedID := strings.TrimSpace(id)
	if requestedID == "" {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	canonicalID, found, err := postgresObjectIDTx(ctx, tx, requestedID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
		return err
	}
	if err := postgresRequireContentMethodTx(ctx, tx, canonicalID, "delete"); err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = $1", canonicalID)
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

func (db *PostgresDB) DeleteObjectAlias(ctx context.Context, aliasID string) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM drs_object_alias WHERE alias_id = $1", aliasID)
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

func (db *PostgresDB) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
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
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}
	var exists string
	err = tx.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = $1", canonicalObjectID).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return err
	}
	if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalObjectID); err != nil {
		return err
	}
	if err := postgresRequireContentMethodTx(ctx, tx, canonicalObjectID, "update"); err != nil {
		return err
	}
	var physicalAlias string
	physicalErr := tx.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = $1", aliasID).Scan(&physicalAlias)
	if physicalErr == nil {
		return fmt.Errorf("%w: alias %q is already a physical object", common.ErrConflict, aliasID)
	}
	if physicalErr != sql.ErrNoRows {
		return physicalErr
	}
	var aliasTarget string
	aliasErr := tx.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = $1", aliasID).Scan(&aliasTarget)
	if aliasErr == nil && aliasTarget != canonicalObjectID {
		return fmt.Errorf("%w: alias %q already points to %q", common.ErrConflict, aliasID, aliasTarget)
	}
	if aliasErr != nil && aliasErr != sql.ErrNoRows {
		return aliasErr
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object_alias(alias_id, object_id)
		VALUES ($1, $2)
		ON CONFLICT(alias_id) DO NOTHING
	`, aliasID, canonicalObjectID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (db *PostgresDB) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	aliasID = strings.TrimSpace(aliasID)
	if aliasID == "" {
		return "", fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	var canonicalID string
	err := db.db.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = $1", aliasID).Scan(&canonicalID)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return "", err
	}
	return canonicalID, nil
}

func (db *PostgresDB) GetObject(ctx context.Context, id string) (*models.InternalObject, error) {
	requestID := strings.TrimSpace(id)
	lookupID := requestID
	resolvedAlias := false

retryLookup:
	// 1. Fetch main record
	var r models.DrsObjectRecord
	var name, version, description sql.NullString
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`, lookupID).Scan(
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
	urlRows, err := db.db.QueryContext(ctx, "SELECT url, type FROM drs_object_access_method WHERE object_id = $1", lookupID)
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
	hashRows, err := db.db.QueryContext(ctx, "SELECT type, checksum FROM drs_object_checksum WHERE object_id = $1", lookupID)
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

func (db *PostgresDB) createObjectLegacy(ctx context.Context, obj *models.InternalObject) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.CleanToBasename(common.StringVal(obj.Name)), common.StringVal(obj.Version), common.StringVal(obj.Description),
	)
	if err != nil {
		return fmt.Errorf("failed to insert drs_object: %w", err)
	}

	if err := insertControlledAccessTx(ctx, tx, obj.Id, objectAccessResources(obj)); err != nil {
		return err
	}
	for _, alias := range normalizeObjectNameAliases(obj) {
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_name_alias (object_id, name_alias) VALUES ($1, $2)`, obj.Id, alias)
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
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, obj.Id, am.AccessUrl.Url, am.Type)
			if err != nil {
				return fmt.Errorf("failed to insert access method: %w", err)
			}
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

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, []string{obj.Id}); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) registerObjectsLegacy(ctx context.Context, objects []models.InternalObject) error {
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
	controlledObjectIDs := make([]string, 0)
	controlledResources := make([]string, 0)

	checksumObjectIDs := make([]string, 0)
	checksumTypes := make([]string, 0)
	checksumValues := make([]string, 0)
	nameAliasObjectIDs := make([]string, 0)
	nameAliasValues := make([]string, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		sizes = append(sizes, obj.Size)
		createdTimes = append(createdTimes, obj.CreatedTime)
		updatedTimes = append(updatedTimes, common.TimeVal(obj.UpdatedTime))
		names = append(names, common.CleanToBasename(common.StringVal(obj.Name)))
		versions = append(versions, common.StringVal(obj.Version))
		descriptions = append(descriptions, common.StringVal(obj.Description))

		seenAccess := make(map[string]struct{})
		for _, resource := range objectAccessResources(&obj) {
			controlledObjectIDs = append(controlledObjectIDs, obj.Id)
			controlledResources = append(controlledResources, resource)
		}
		for _, alias := range normalizeObjectNameAliases(&obj) {
			nameAliasObjectIDs = append(nameAliasObjectIDs, obj.Id)
			nameAliasValues = append(nameAliasValues, alias)
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
				accessObjectIDs = append(accessObjectIDs, obj.Id)
				accessURLs = append(accessURLs, am.AccessUrl.Url)
				accessTypes = append(accessTypes, string(am.Type))
			}
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_controlled_access WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear controlled access: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_checksum WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear checksums: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_name_alias WHERE object_id = ANY($1)`, pq.Array(ids)); err != nil {
		return fmt.Errorf("failed bulk clear name aliases: %w", err)
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
	if len(controlledObjectIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drs_object_controlled_access (object_id, resource)
			SELECT * FROM UNNEST($1::text[], $2::text[])`,
			pq.Array(controlledObjectIDs), pq.Array(controlledResources),
		); err != nil {
			return fmt.Errorf("failed bulk insert controlled access: %w", err)
		}
	}
	if len(nameAliasObjectIDs) > 0 {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drs_object_name_alias (object_id, name_alias)
			SELECT * FROM UNNEST($1::text[], $2::text[])`,
			pq.Array(nameAliasObjectIDs), pq.Array(nameAliasValues),
		); err != nil {
			return fmt.Errorf("failed bulk insert name aliases: %w", err)
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

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, ids); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) GetBulkObjects(ctx context.Context, ids []string) ([]models.InternalObject, error) {
	if len(ids) == 0 {
		return []models.InternalObject{}, nil
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

func (db *PostgresDB) GetObjectsByChecksum(ctx context.Context, checksum string) ([]models.InternalObject, error) {
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

func (db *PostgresDB) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]models.InternalObject, error) {
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

func (db *PostgresDB) ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error) {
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
	rows, err := db.db.QueryContext(ctx, `
		SELECT DISTINCT replace(lower(trim(c.checksum)), 'sha256:', ''), c.object_id
		FROM drs_object_checksum c
		INNER JOIN drs_object_controlled_access ca ON ca.object_id = c.object_id
		WHERE ca.resource = $1 AND replace(lower(trim(c.type)), '-', '') = $2
		  AND replace(lower(trim(c.checksum)), 'sha256:', '') = ANY($3)
		ORDER BY 1, 2`, resource, "sha256", pq.Array(normalized))
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

func (db *PostgresDB) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
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
			WHERE ca.resource = $1
			ORDER BY ca.object_id`, resource)
	} else {
		scopeCondition, scopeArgs, scopeErr := postgresScopeResourceCondition("ca.resource", organization, "")
		if scopeErr != nil {
			return nil, scopeErr
		}
		rows, err = db.db.QueryContext(ctx, `
				SELECT DISTINCT ca.object_id
				FROM drs_object_controlled_access ca
				INNER JOIN drs_object o ON o.id = ca.object_id
				WHERE `+postgresRebindQuestionPlaceholders(scopeCondition, 1)+`
				ORDER BY ca.object_id`, scopeArgs...)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func (db *PostgresDB) ListObjectIDsPageByScope(ctx context.Context, organization, project, startAfter string, limit, offset int) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	startAfter = strings.TrimSpace(startAfter)
	if limit <= 0 {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}

	queryArgs := make([]any, 0, 4)
	conditions := make([]string, 0, 2)
	baseQuery := `SELECT id FROM drs_object`
	orderBy := ` ORDER BY id`
	objectIDExpr := "id"

	if organization != "" {
		scopeCondition, scopeArgs, err := postgresScopeResourceCondition("ca.resource", organization, project)
		if err != nil {
			return nil, err
		}
		queryArgs = append(queryArgs, scopeArgs...)
		baseQuery = `
			SELECT DISTINCT ca.object_id AS id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
		`
		objectIDExpr = "ca.object_id"
		conditions = append(conditions, postgresRebindQuestionPlaceholders(scopeCondition, len(queryArgs)-len(scopeArgs)+1))
		orderBy = ` ORDER BY ca.object_id`
	}
	if startAfter != "" {
		queryArgs = append(queryArgs, startAfter)
		conditions = append(conditions, fmt.Sprintf("%s > $%d", objectIDExpr, len(queryArgs)))
	}
	query := baseQuery
	if len(conditions) > 0 {
		query += ` WHERE ` + strings.Join(conditions, ` AND `)
	}
	query += orderBy
	queryArgs = append(queryArgs, limit, offset)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(queryArgs)-1, len(queryArgs))
	rows, err := db.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *PostgresDB) ListObjectIDsByResources(ctx context.Context, resources []string, includeUnscoped bool) ([]string, error) {
	resources = sycommon.NormalizeAccessResources(resources)
	if len(resources) == 0 && !includeUnscoped {
		return []string{}, nil
	}
	rows, err := db.db.QueryContext(ctx, `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE (
			COALESCE(array_length($1::text[], 1), 0) > 0
			AND EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca
				WHERE ca.object_id = o.id AND ca.resource = ANY($1)
			)
		) OR (
			$2
			AND NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca
				WHERE ca.object_id = o.id
			)
		)
		ORDER BY o.id`, pq.Array(resources), includeUnscoped)
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

func (db *PostgresDB) ListObjectIDsPageByResources(ctx context.Context, resources []string, includeUnscoped bool, startAfter string, limit, offset int) ([]string, error) {
	resources = sycommon.NormalizeAccessResources(resources)
	startAfter = strings.TrimSpace(startAfter)
	if limit <= 0 || (len(resources) == 0 && !includeUnscoped) {
		return []string{}, nil
	}
	if offset < 0 {
		offset = 0
	}
	queryArgs := []any{pq.Array(resources), includeUnscoped}
	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE ((
			COALESCE(array_length($1::text[], 1), 0) > 0
			AND EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca
				WHERE ca.object_id = o.id AND ca.resource = ANY($1)
			)
		) OR (
			$2
			AND NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca
				WHERE ca.object_id = o.id
			)
		))
	`
	if startAfter != "" {
		queryArgs = append(queryArgs, startAfter)
		query += fmt.Sprintf(" AND o.id > $%d", len(queryArgs))
	}
	query += " ORDER BY o.id"
	queryArgs = append(queryArgs, limit, offset)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(queryArgs)-1, len(queryArgs))
	rows, err := db.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *PostgresDB) ListObjectIDsPageByChecksum(ctx context.Context, checksum, checksumType, organization, project, startAfter string, limit, offset int, resources []string, includeUnscoped, restrictToResources bool) ([]string, error) {
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
		conditions = append(conditions, fmt.Sprintf(`(
			o.id = $%d OR EXISTS (
				SELECT 1
				FROM drs_object_checksum c2
				WHERE c2.object_id = o.id AND c2.checksum = $%d
			)
		)`, len(args), len(args)))
	} else {
		args = append(args, checksumType)
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1
			FROM drs_object_checksum c2
			WHERE c2.object_id = o.id AND c2.checksum = $1 AND c2.type = $%d
		)`, len(args)))
	}
	if organization != "" {
		scopeCondition, scopeArgs, err := postgresScopeResourceCondition("ca_scope.resource", organization, project)
		if err != nil {
			return nil, err
		}
		args = append(args, scopeArgs...)
		conditions = append(conditions, fmt.Sprintf(`EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND %s
		)`, postgresRebindQuestionPlaceholders(scopeCondition, len(args)-len(scopeArgs)+1)))
	}
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return []string{}, nil
		}
		args = append(args, pq.Array(resources), includeUnscoped)
		conditions = append(conditions, fmt.Sprintf(`(
			(COALESCE(array_length($%d::text[], 1), 0) > 0 AND EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource = ANY($%d)
			)) OR (
				$%d AND NOT EXISTS (
					SELECT 1
					FROM drs_object_controlled_access ca_auth
					WHERE ca_auth.object_id = o.id
				)
			)
		)`, len(args)-1, len(args)-1, len(args)))
	}
	if startAfter != "" {
		args = append(args, startAfter)
		conditions = append(conditions, fmt.Sprintf("o.id > $%d", len(args)))
	}

	query := `
		SELECT o.id
		FROM drs_object o
		WHERE ` + strings.Join(conditions, ` AND `) + `
		ORDER BY o.id`
	args = append(args, limit, offset)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *PostgresDB) ListObjectIDsByScopeAndResources(ctx context.Context, organization, project string, resources []string, restrictToResources bool) ([]string, error) {
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

	scopeCondition, scopeArgs, err := postgresScopeResourceCondition("ca_scope.resource", organization, project)
	if err != nil {
		return nil, err
	}
	args := make([]any, 0, 3)
	args = append(args, scopeArgs...)
	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND ` + postgresRebindQuestionPlaceholders(scopeCondition, 1) + `
		)`
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 {
			return []string{}, nil
		}
		args = append(args, pq.Array(resources))
		query += fmt.Sprintf(`
		AND EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_auth
			WHERE ca_auth.object_id = o.id AND ca_auth.resource = ANY($%d)
		)`, len(args))
	}
	query += ` ORDER BY o.id`
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *PostgresDB) ListObjectIDsByChecksumsAndResources(ctx context.Context, checksums []string, resources []string, includeUnscoped, restrictToResources bool) (map[string][]string, error) {
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

	args := make([]any, 0, 4)
	args = append(args, pq.Array(checksums))
	query := `
		WITH matched AS (
			SELECT id AS object_id, id AS match_key
			FROM drs_object
			WHERE id = ANY($1)
			UNION
			SELECT c.object_id, c.checksum AS match_key
			FROM drs_object_checksum c
			WHERE c.checksum = ANY($1)
		)
		SELECT m.match_key, m.object_id
		FROM matched m
		INNER JOIN drs_object o ON o.id = m.object_id`
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return map[string][]string{}, nil
		}
		args = append(args, pq.Array(resources), includeUnscoped)
		query += `
		WHERE (
			COALESCE(array_length($2::text[], 1), 0) > 0
			AND EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource = ANY($2)
			)
		) OR (
			$3
			AND NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)
		)`
	}
	query += ` ORDER BY m.match_key, m.object_id`
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanChecksumMatchRows(rows)
}

func (db *PostgresDB) ListObjectIDsPageByURL(ctx context.Context, objectURL, organization, project, startAfter string, limit, offset int, resources []string, includeUnscoped, restrictToResources bool) ([]string, error) {
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
	conditions := []string{"am.url = $1"}
	if organization != "" {
		scopeCondition, scopeArgs, err := postgresScopeResourceCondition("ca_scope.resource", organization, project)
		if err != nil {
			return nil, err
		}
		args = append(args, scopeArgs...)
		conditions = append(conditions, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND `+postgresRebindQuestionPlaceholders(scopeCondition, len(args)-len(scopeArgs)+1)+`
		)`)
	}
	if restrictToResources {
		resources = sycommon.NormalizeAccessResources(resources)
		if len(resources) == 0 && !includeUnscoped {
			return []string{}, nil
		}
		args = append(args, pq.Array(resources), includeUnscoped)
		resourcesArg := len(args) - 1
		includeUnscopedArg := len(args)
		conditions = append(conditions, fmt.Sprintf(`(
			(
				COALESCE(array_length($%d::text[], 1), 0) > 0
				AND EXISTS (
					SELECT 1
					FROM drs_object_controlled_access ca_auth
					WHERE ca_auth.object_id = o.id AND ca_auth.resource = ANY($%d)
				)
			) OR (
				$%d
				AND NOT EXISTS (
					SELECT 1
					FROM drs_object_controlled_access ca_auth
					WHERE ca_auth.object_id = o.id
				)
			)
		)`, resourcesArg, resourcesArg, includeUnscopedArg))
	}
	if startAfter != "" {
		args = append(args, startAfter)
		conditions = append(conditions, fmt.Sprintf("o.id > $%d", len(args)))
	}

	args = append(args, limit, offset)
	query := `
		SELECT DISTINCT o.id
		FROM drs_object o
		INNER JOIN drs_object_access_method am ON am.object_id = o.id
		WHERE ` + strings.Join(conditions, " AND ") + fmt.Sprintf(`
		ORDER BY o.id
		LIMIT $%d OFFSET $%d`, len(args)-1, len(args))
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *PostgresDB) ListBucketVisibilityRows(ctx context.Context, resources []string, includeUnscoped, restrictToResources bool) ([]models.BucketVisibilityRow, error) {
	args := make([]any, 0, 2)
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
		args = append(args, pq.Array(resources), includeUnscoped)
		query += `
		WHERE (
			COALESCE(array_length($1::text[], 1), 0) > 0
			AND EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND ca_auth.resource = ANY($1)
			)
		) OR (
			$2
			AND NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)
		)`
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

func (db *PostgresDB) BulkDeleteObjects(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}
	canonicalIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, rawID := range ids {
		canonicalID, found, resolveErr := postgresObjectIDTx(ctx, tx, strings.TrimSpace(rawID))
		if resolveErr != nil {
			return resolveErr
		}
		if !found {
			continue
		}
		if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
			return err
		}
		if err := postgresRequireContentMethodTx(ctx, tx, canonicalID, "delete"); err != nil {
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

	if _, err := tx.ExecContext(ctx, "DELETE FROM drs_object WHERE id = ANY($1)", pq.Array(canonicalIDs)); err != nil {
		return err
	}
	return tx.Commit()
}

func postgresScopeResourceCondition(column, organization, project string) (string, []any, error) {
	resource, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return "", nil, err
	}
	if strings.TrimSpace(project) != "" {
		return column + " = ?", []any{resource}, nil
	}
	return "(" + column + " = ? OR " + column + " LIKE ? ESCAPE '\\')", []any{resource, postgresLikeEscape(resource+"/project/") + "%"}, nil
}

func postgresLikeEscape(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(value)
}

func postgresRebindQuestionPlaceholders(query string, start int) string {
	var b strings.Builder
	next := start
	for _, r := range query {
		if r == '?' {
			b.WriteString(fmt.Sprintf("$%d", next))
			next++
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
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

func (db *PostgresDB) fetchObjectsByIDsOrChecksums(ctx context.Context, ids []string, checksums []string) (map[string]*models.InternalObject, error) {
	if len(ids) == 0 && len(checksums) == 0 {
		return map[string]*models.InternalObject{}, nil
	}

	shaQueries := make([]string, 0, len(checksums))
	genericQueries := make([]string, 0, len(checksums))
	for _, checksum := range checksums {
		if normalized, ok := common.NormalizeSHA256Query(checksum); ok {
			shaQueries = append(shaQueries, normalized)
		} else {
			genericQueries = append(genericQueries, strings.TrimSpace(checksum))
		}
	}
	rows, err := db.db.QueryContext(ctx, `
		SELECT
			o.id,
			o.size,
			o.created_time,
			o.updated_time,
			o.name,
			o.version,
			o.description
		FROM drs_object o
		WHERE (
			(COALESCE(array_length($1::text[], 1), 0) > 0 AND o.id = ANY($1))
			OR
			(COALESCE(array_length($2::text[], 1), 0) > 0 AND (
				o.id = ANY($2)
				OR EXISTS (SELECT 1 FROM drs_object_checksum c2
					WHERE c2.object_id = o.id
					  AND replace(lower(trim(c2.type)), '-', '') = 'sha256'
					  AND replace(lower(trim(c2.checksum)), 'sha256:', '') = ANY($3))
				OR EXISTS (SELECT 1 FROM drs_object_checksum c2
					WHERE c2.object_id = o.id AND c2.checksum = ANY($4))
			))
		)`,
		pq.Array(ids), pq.Array(checksums), pq.Array(shaQueries), pq.Array(genericQueries),
	)
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

func (db *PostgresDB) attachBulkAccessMethods(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, url, type
		FROM drs_object_access_method
		WHERE object_id = ANY($1)
		ORDER BY object_id`, pq.Array(sortedObjectIDs(objectsByID)))
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
		am := drs.AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: accessURL},
			Type:     drs.AccessMethodType(accessType),
			AccessId: common.Ptr(common.AccessMethodID(accessType, accessURL)),
		}
		*obj.DrsObject.AccessMethods = append(*obj.DrsObject.AccessMethods, am)
	}
	return rows.Err()
}

func (db *PostgresDB) attachBulkChecksums(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, type, checksum
		FROM drs_object_checksum
		WHERE object_id = ANY($1)
		ORDER BY object_id`, pq.Array(sortedObjectIDs(objectsByID)))
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

func (db *PostgresDB) UpdateObjectAccessMethods(ctx context.Context, objectID string, accessMethods []drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}
	canonicalID, found, err := postgresObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
		return err
	}
	if err := postgresRequireContentMethodTx(ctx, tx, canonicalID, "update"); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = $1", canonicalID)
	if err != nil {
		return err
	}

	for _, am := range accessMethods {
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
			continue
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, canonicalID, am.AccessUrl.Url, am.Type)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (db *PostgresDB) RemoveObjectControlledAccess(ctx context.Context, objectID, resource string) error {
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
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}
	canonicalID, found, err := postgresObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
	if err != nil {
		return err
	}
	if !found {
		return common.ErrNotFound
	}
	if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
		return err
	}
	if !authz.HasMethodAccess(ctx, "update", []string{resource}) {
		return common.ErrUnauthorized
	}

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM drs_object_controlled_access WHERE object_id = $1 AND resource = $2`, canonicalID, resource).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return common.ErrNotFound
	}
	currentResources, err := postgresResourcesTx(ctx, tx, canonicalID)
	if err != nil {
		return err
	}
	publicRead, err := postgresPublicReadTx(ctx, tx, canonicalID, len(currentResources) == 0)
	if err != nil {
		return err
	}
	if err := postgresSetPublicReadTx(ctx, tx, canonicalID, publicRead); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_controlled_access WHERE object_id = $1 AND resource = $2`, canonicalID, resource); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *PostgresDB) RemoveObjectControlledAccessBulk(ctx context.Context, objectIDs []string, resource string) (int, error) {
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
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return 0, err
	}
	orgWide := !strings.Contains(resource, "/project/")
	if !orgWide && !authz.HasMethodAccess(ctx, "delete", []string{resource}) {
		return 0, common.ErrUnauthorized
	}
	seen := make(map[string]struct{}, len(objectIDs))
	removed := 0
	for _, rawID := range objectIDs {
		canonicalID, found, resolveErr := postgresObjectIDTx(ctx, tx, strings.TrimSpace(rawID))
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
		if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
			return 0, err
		}
		currentResources, err := postgresResourcesTx(ctx, tx, canonicalID)
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
			if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_controlled_access WHERE object_id = $1 AND resource = $2`, canonicalID, currentResource); err != nil {
				return 0, err
			}
			removed++
			objectRemoved++
		}
		if objectRemoved == 0 {
			continue
		}
		currentResources, err = postgresResourcesTx(ctx, tx, canonicalID)
		if err != nil {
			return 0, err
		}
		publicRead, err := postgresPublicReadTx(ctx, tx, canonicalID, len(currentResources) == 0)
		if err != nil {
			return 0, err
		}
		if err := postgresSetPublicReadTx(ctx, tx, canonicalID, publicRead); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return removed, nil
}

func (db *PostgresDB) BulkUpdateAccessMethods(ctx context.Context, updates map[string][]drs.AccessMethod) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := lockContentWriteTx(ctx, tx); err != nil {
		return err
	}

	for objectID, methods := range updates {
		canonicalID, found, resolveErr := postgresObjectIDTx(ctx, tx, strings.TrimSpace(objectID))
		if resolveErr != nil {
			return resolveErr
		}
		if !found {
			return common.ErrNotFound
		}
		if err := postgresEnsureNoLegacyDuplicateTx(ctx, tx, canonicalID); err != nil {
			return err
		}
		if err := postgresRequireContentMethodTx(ctx, tx, canonicalID, "update"); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "DELETE FROM drs_object_access_method WHERE object_id = $1", canonicalID)
		if err != nil {
			return err
		}
		for _, am := range methods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, canonicalID, am.AccessUrl.Url, am.Type)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
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

func insertControlledAccessTx(ctx context.Context, tx *sql.Tx, objectID string, resources []string) error {
	for _, resource := range sycommon.NormalizeAccessResources(resources) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO drs_object_controlled_access (object_id, resource) VALUES ($1, $2)`, objectID, resource); err != nil {
			return fmt.Errorf("failed to insert controlled access: %w", err)
		}
	}
	return nil
}

func (db *PostgresDB) controlledAccessForObject(ctx context.Context, objectID string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `SELECT resource FROM drs_object_controlled_access WHERE object_id = $1 ORDER BY resource`, objectID)
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

func (db *PostgresDB) nameAliasesForObject(ctx context.Context, objectID string) ([]string, error) {
	rows, err := db.db.QueryContext(ctx, `SELECT name_alias FROM drs_object_name_alias WHERE object_id = $1 ORDER BY name_alias`, objectID)
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

func (db *PostgresDB) attachControlledAccess(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	ids := make([]string, 0, len(objectsByID))
	for id := range objectsByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, resource
		FROM drs_object_controlled_access
		WHERE object_id = ANY($1)
		ORDER BY object_id, resource`, pq.Array(ids))
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

func (db *PostgresDB) attachPublicRead(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, public_read
		FROM drs_object_read_policy
		WHERE object_id = ANY($1)`, pq.Array(sortedObjectIDs(objectsByID)))
	if err != nil {
		return err
	}
	defer rows.Close()
	known := make(map[string]bool, len(objectsByID))
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

func (db *PostgresDB) attachNameAliases(ctx context.Context, objectsByID map[string]*models.InternalObject) error {
	if len(objectsByID) == 0 {
		return nil
	}
	rows, err := db.db.QueryContext(ctx, `
		SELECT object_id, name_alias
		FROM drs_object_name_alias
		WHERE object_id = ANY($1)
		ORDER BY object_id, name_alias`, pq.Array(sortedObjectIDs(objectsByID)))
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
