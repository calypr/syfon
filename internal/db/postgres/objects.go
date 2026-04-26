package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
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

	canonicalID := strings.TrimSpace(id)
	if canonicalID == "" {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}

	if err := tx.QueryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = $1", canonicalID).Scan(&canonicalID); err != nil && !errors.Is(err, sql.ErrNoRows) {
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
	result, err := db.db.ExecContext(ctx, "DELETE FROM drs_object_alias WHERE alias_id = $1", aliasID)
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

func (db *PostgresDB) CreateObjectAlias(ctx context.Context, aliasID, canonicalObjectID string) error {
	aliasID = strings.TrimSpace(aliasID)
	canonicalObjectID = strings.TrimSpace(canonicalObjectID)
	if aliasID == "" || canonicalObjectID == "" {
		return fmt.Errorf("alias_id and canonical object id are required")
	}
	if aliasID == canonicalObjectID {
		return nil
	}
	var exists string
	err := db.db.QueryRowContext(ctx, "SELECT id FROM drs_object WHERE id = $1", canonicalObjectID).Scan(&exists)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: object not found", common.ErrNotFound)
	}
	if err != nil {
		return err
	}
	_, err = db.db.ExecContext(ctx, `
		INSERT INTO drs_object_alias(alias_id, object_id)
		VALUES ($1, $2)
		ON CONFLICT(alias_id) DO UPDATE SET object_id=EXCLUDED.object_id
	`, aliasID, canonicalObjectID)
	return err
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
	err := db.db.QueryRowContext(ctx, `
		SELECT id, size, created_time, updated_time, name, version, description
		FROM drs_object WHERE id = $1`, lookupID).Scan(
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

	// 2. Fetch URLs (Access Methods)
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
		amID := t
		*obj.AccessMethods = append(*obj.AccessMethods, drs.AccessMethod{
			AccessUrl: &struct {
				Headers *[]string `json:"headers,omitempty"`
				Url     string    `json:"url"`
			}{Url: u},
			Type:     drs.AccessMethodType(t),
			AccessId: &amID,
		})
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

	// 4. Fetch object-level authz scopes.
	authzRows, err := db.db.QueryContext(ctx, "SELECT org, project FROM drs_object_authz WHERE object_id = $1", lookupID)
	if err != nil {
		return nil, err
	}
	defer authzRows.Close()
	authzMap := make(map[string][]string)
	seenAuthz := make(map[string]struct{})
	for authzRows.Next() {
		var org, project string
		if err := authzRows.Scan(&org, &project); err != nil {
			return nil, err
		}
		if org == "" {
			continue
		}
		key := org + "|" + project
		if _, ok := seenAuthz[key]; ok {
			continue
		}
		seenAuthz[key] = struct{}{}
		if project == "" {
			if _, ok := authzMap[org]; !ok {
				authzMap[org] = []string{}
			}
		} else {
			authzMap[org] = append(authzMap[org], project)
		}
	}
	if err := authzRows.Err(); err != nil {
		return nil, err
	}
	if len(authzMap) > 0 {
		obj.Authorizations = authzMap
		if obj.AccessMethods != nil {
			for i := range *obj.AccessMethods {
				am := &(*obj.AccessMethods)[i]
				if am.Authorizations == nil {
					am.Authorizations = &authzMap
				}
			}
		}
	}
	// 5. RBAC Check (gen3 mode only)
	if !authz.IsGen3Mode(ctx) {
		return obj, nil
	}

	// Optimized in SQL for gen3 mode: reconstruct resource paths from org/project columns
	// and compare against the user's authorized resources.
	userResources := authz.GetUserAuthz(ctx)

	var count int
	err = db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM drs_object o
		WHERE o.id = $1 AND (
			NOT EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id)
			OR EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id
				AND ('/programs/' || a.org || CASE WHEN a.project != '' THEN '/projects/' || a.project ELSE '' END) = ANY($2))
		)`, lookupID, pq.Array(userResources)).Scan(&count)

	if err != nil {
		return nil, fmt.Errorf("authorization check failed: %w", err)
	}
	if count == 0 {
		return nil, fmt.Errorf("%w: access to object denied", common.ErrUnauthorized)
	}

	return obj, nil
}

func (db *PostgresDB) CreateObject(ctx context.Context, obj *models.InternalObject) error {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert main record
	_, err = tx.ExecContext(ctx, `
		INSERT INTO drs_object (id, size, created_time, updated_time, name, version, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		obj.Id, obj.Size, obj.CreatedTime, common.TimeVal(obj.UpdatedTime), common.StringVal(obj.Name), common.StringVal(obj.Version), common.StringVal(obj.Description),
	)
	if err != nil {
		return fmt.Errorf("failed to insert drs_object: %w", err)
	}

	// Insert URLs
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
				continue
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_access_method (object_id, url, type) VALUES ($1, $2, $3)`, obj.Id, am.AccessUrl.Url, am.Type)
			if err != nil {
				return fmt.Errorf("failed to insert url: %w", err)
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

	// Insert Authz
	for org, projects := range obj.Authorizations {
		if len(projects) == 0 {
			_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, org, project) VALUES ($1, $2, '')`, obj.Id, org)
			if err != nil {
				return fmt.Errorf("failed to insert authz: %w", err)
			}
		} else {
			for _, p := range projects {
				_, err = tx.ExecContext(ctx, `INSERT INTO drs_object_authz (object_id, org, project) VALUES ($1, $2, $3)`, obj.Id, org, p)
				if err != nil {
					return fmt.Errorf("failed to insert authz: %w", err)
				}
			}
		}
	}

	if err := db.flushObjectUsageEventsForIDsTx(ctx, tx, []string{obj.Id}); err != nil {
		return fmt.Errorf("failed to apply object usage events: %w", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) RegisterObjects(ctx context.Context, objects []models.InternalObject) error {
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
	authzOrgs := make([]string, 0)
	authzProjects := make([]string, 0)

	for _, obj := range objects {
		ids = append(ids, obj.Id)
		sizes = append(sizes, obj.Size)
		createdTimes = append(createdTimes, obj.CreatedTime)
		updatedTimes = append(updatedTimes, common.TimeVal(obj.UpdatedTime))
		names = append(names, common.StringVal(obj.Name))
		versions = append(versions, common.StringVal(obj.Version))
		descriptions = append(descriptions, common.StringVal(obj.Description))

		seenAccess := make(map[string]struct{})
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

		seenAuthz := make(map[string]struct{})
		for org, projects := range obj.Authorizations {
			if len(projects) == 0 {
				key := org + "|"
				if _, ok := seenAuthz[key]; !ok {
					seenAuthz[key] = struct{}{}
					authzObjectIDs = append(authzObjectIDs, obj.Id)
					authzOrgs = append(authzOrgs, org)
					authzProjects = append(authzProjects, "")
				}
			} else {
				for _, p := range projects {
					key := org + "|" + p
					if _, ok := seenAuthz[key]; !ok {
						seenAuthz[key] = struct{}{}
						authzObjectIDs = append(authzObjectIDs, obj.Id)
						authzOrgs = append(authzOrgs, org)
						authzProjects = append(authzProjects, p)
					}
				}
			}
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
			INSERT INTO drs_object_authz (object_id, org, project)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])`,
			pq.Array(authzObjectIDs), pq.Array(authzOrgs), pq.Array(authzProjects),
		); err != nil {
			return fmt.Errorf("failed bulk insert authz: %w", err)
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

func (db *PostgresDB) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
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

	var (
		rows *sql.Rows
		err  error
	)
	if project != "" {
		rows, err = db.db.QueryContext(ctx, `
			SELECT DISTINCT a.object_id
			FROM drs_object_authz a
			INNER JOIN drs_object o ON o.id = a.object_id
			WHERE a.org = $1 AND a.project = $2`, organization, project)
	} else {
		rows, err = db.db.QueryContext(ctx, `
			SELECT DISTINCT a.object_id
			FROM drs_object_authz a
			INNER JOIN drs_object o ON o.id = a.object_id
			WHERE a.org = $1`, organization)
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

func (db *PostgresDB) fetchObjectsByIDsOrChecksums(ctx context.Context, ids []string, checksums []string) (map[string]*models.InternalObject, error) {
	if len(ids) == 0 && len(checksums) == 0 {
		return map[string]*models.InternalObject{}, nil
	}

	gen3Mode := authz.IsGen3Mode(ctx)
	userResources := authz.GetUserAuthz(ctx)
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
			oa.org,
			oa.project
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
			$4::boolean = false
			OR
			NOT EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id)
			OR EXISTS (SELECT 1 FROM drs_object_authz a WHERE a.object_id = o.id
				AND ('/programs/' || a.org || CASE WHEN a.project != '' THEN '/projects/' || a.project ELSE '' END) = ANY($3))
		)`,
		pq.Array(ids), pq.Array(checksums), pq.Array(userResources), gen3Mode,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bulk objects: %w", err)
	}
	defer rows.Close()

	objectsByID := make(map[string]*models.InternalObject)
	seenAccess := make(map[string]map[string]struct{})
	seenChecksum := make(map[string]map[string]struct{})
	seenAuthz := make(map[string]map[string]struct{})

	for rows.Next() {
		var (
			id, name, version, description              string
			size                                        int64
			createdTime, updatedTime                    time.Time
			accessURL, accessType, checksumType, sumVal sql.NullString
			authzOrg, authzProject                      sql.NullString
		)
		if err := rows.Scan(
			&id, &size, &createdTime, &updatedTime, &name, &version, &description,
			&accessURL, &accessType, &checksumType, &sumVal, &authzOrg, &authzProject,
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
			seenAuthz[id] = make(map[string]struct{})
		}

		if accessURL.Valid && accessType.Valid {
			key := accessType.String + "|" + accessURL.String
			if _, exists := seenAccess[id][key]; !exists {
				seenAccess[id][key] = struct{}{}
				if obj.DrsObject.AccessMethods == nil {
					obj.DrsObject.AccessMethods = &[]drs.AccessMethod{}
				}
				amID := accessType.String
				*obj.DrsObject.AccessMethods = append(*obj.DrsObject.AccessMethods, drs.AccessMethod{
					AccessUrl: &struct {
						Headers *[]string `json:"headers,omitempty"`
						Url     string    `json:"url"`
					}{Url: accessURL.String},
					Type:     drs.AccessMethodType(accessType.String),
					AccessId: &amID,
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

		if authzOrg.Valid && strings.TrimSpace(authzOrg.String) != "" {
			org := authzOrg.String
			proj := ""
			if authzProject.Valid {
				proj = authzProject.String
			}
			key := org + "|" + proj
			if _, exists := seenAuthz[id][key]; !exists {
				seenAuthz[id][key] = struct{}{}
				if obj.Authorizations == nil {
					obj.Authorizations = make(map[string][]string)
				}
				if proj == "" {
					if _, ok := obj.Authorizations[org]; !ok {
						obj.Authorizations[org] = []string{}
					}
				} else {
					obj.Authorizations[org] = append(obj.Authorizations[org], proj)
				}
			}
		}
	}

	for _, obj := range objectsByID {
		if len(obj.Authorizations) == 0 || obj.DrsObject.AccessMethods == nil {
			continue
		}
		for i := range *obj.DrsObject.AccessMethods {
			am := &(*obj.DrsObject.AccessMethods)[i]
			if am.Authorizations == nil {
				am.Authorizations = &obj.Authorizations
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
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
		if am.AccessUrl == nil || am.AccessUrl.Url == "" {
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
			if am.AccessUrl == nil || am.AccessUrl.Url == "" {
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
