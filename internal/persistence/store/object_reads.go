package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/errorapi"
	clientaccess "github.com/calypr/syfon/client/access"

	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/storage/address"
)

func (db *Store) ResolveObjectAlias(ctx context.Context, aliasID string) (string, error) {
	aliasID = strings.TrimSpace(aliasID)
	if aliasID == "" {
		return "", errorapi.ErrObjectNotFound
	}
	var canonicalID string
	err := db.queryRowContext(ctx, "SELECT object_id FROM drs_object_alias WHERE alias_id = ?", aliasID).Scan(&canonicalID)
	if err == sql.ErrNoRows {
		return "", errorapi.ErrObjectNotFound
	}
	if err != nil {
		return "", err
	}
	return canonicalID, nil
}

// ResolveObjectIDs maps physical IDs and aliases to canonical physical IDs in
// one batch, omitting identifiers that do not exist.
func (db *Store) ResolveObjectIDs(ctx context.Context, ids []string) (map[string]string, error) {
	requested := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, rawID := range ids {
		id := strings.TrimSpace(rawID)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		requested = append(requested, id)
	}
	resolved := make(map[string]string, len(requested))
	if len(requested) == 0 {
		return resolved, nil
	}

	condition, args := db.dialect.ListArgs("id", requested)
	rows, err := db.queryContext(ctx, `SELECT id FROM drs_object WHERE `+condition, args...)
	if err != nil {
		return nil, err
	}
	physical := make(map[string]*drs.DrsObject, len(requested))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		physical[id] = &drs.DrsObject{Id: id}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	aliases, err := db.resolveObjectAliases(ctx, requested, physical)
	if err != nil {
		return nil, err
	}
	for _, id := range requested {
		if object, ok := physical[id]; ok {
			resolved[id] = object.Id
			continue
		}
		if canonicalID, ok := aliases[id]; ok {
			resolved[id] = canonicalID
		}
	}
	return resolved, nil
}

func (db *Store) GetBulkObjects(ctx context.Context, ids []string) ([]drs.DrsObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, ids, nil)
	if err != nil {
		return nil, err
	}
	aliasTargets, err := db.resolveObjectAliases(ctx, ids, objectsByID)
	if err != nil {
		return nil, err
	}
	canonicalIDs := make([]string, 0, len(aliasTargets))
	seenCanonicalIDs := make(map[string]struct{}, len(aliasTargets))
	for _, canonicalID := range aliasTargets {
		if canonicalID == "" {
			continue
		}
		if _, loaded := objectsByID[canonicalID]; loaded {
			continue
		}
		if _, seen := seenCanonicalIDs[canonicalID]; seen {
			continue
		}
		seenCanonicalIDs[canonicalID] = struct{}{}
		canonicalIDs = append(canonicalIDs, canonicalID)
	}
	if len(canonicalIDs) > 0 {
		canonicalObjects, fetchErr := db.fetchObjectsByIDsOrChecksums(ctx, canonicalIDs, nil)
		if fetchErr != nil {
			return nil, fetchErr
		}
		for canonicalID, obj := range canonicalObjects {
			objectsByID[canonicalID] = obj
		}
	}

	objects := make([]drs.DrsObject, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		obj, ok := objectsByID[id]
		if !ok {
			canonicalID, aliased := aliasTargets[strings.TrimSpace(id)]
			if aliased {
				obj, ok = objectsByID[canonicalID]
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

// ListRepairCandidateObjectIDs pages records stored under one S3 project
// prefix that do not yet have the requested controlled-access association.
func (db *Store) ListRepairCandidateObjectIDs(ctx context.Context, query objects.RepairCandidateQuery) ([]string, error) {
	query.Scope.Organization = strings.TrimSpace(query.Scope.Organization)
	query.Scope.Project = strings.TrimSpace(query.Scope.Project)
	query.Bucket = strings.TrimSpace(query.Bucket)
	query.Prefix = strings.Trim(strings.TrimSpace(query.Prefix), "/")
	query.StartAfter = strings.TrimSpace(query.StartAfter)
	if query.Scope.Organization == "" || query.Scope.Project == "" || query.Bucket == "" {
		return nil, fmt.Errorf("repair candidate scope and bucket are required")
	}
	if query.Limit <= 0 {
		return []string{}, nil
	}
	resource, err := clientaccess.ResourcePath(query.Scope.Organization, query.Scope.Project)
	if err != nil {
		return nil, err
	}

	root := strings.TrimRight(address.BucketToURL(query.Bucket, ""), "/")
	if query.Prefix != "" {
		root = address.BucketToURL(query.Bucket, query.Prefix)
	}
	args := []any{root, likeEscape(root+"/") + "%", resource}
	conditions := []string{
		"(am.url = ? OR am.url LIKE ? ESCAPE '\\')",
		`NOT EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id AND ca.resource = ?
		)`,
	}
	if query.StartAfter != "" {
		conditions = append(conditions, "o.id > ?")
		args = append(args, query.StartAfter)
	}
	args = append(args, query.Limit)
	rows, err := db.queryContext(ctx, `
		SELECT DISTINCT o.id
		FROM drs_object o
		INNER JOIN drs_object_access_method am ON am.object_id = o.id
		WHERE `+strings.Join(conditions, " AND ")+`
		ORDER BY o.id
		LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *Store) resolveObjectAliases(ctx context.Context, ids []string, objectsByID map[string]*drs.DrsObject) (map[string]string, error) {
	aliasIDs := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		aliasID := strings.TrimSpace(id)
		if aliasID == "" {
			continue
		}
		if _, exists := objectsByID[aliasID]; exists {
			continue
		}
		if _, exists := seen[aliasID]; exists {
			continue
		}
		seen[aliasID] = struct{}{}
		aliasIDs = append(aliasIDs, aliasID)
	}
	if len(aliasIDs) == 0 {
		return map[string]string{}, nil
	}

	condition, args := db.dialect.ListArgs("alias_id", aliasIDs)
	rows, err := db.queryContext(ctx, `SELECT alias_id, object_id FROM drs_object_alias WHERE `+condition, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	resolved := make(map[string]string, len(aliasIDs))
	for rows.Next() {
		var aliasID, canonicalID string
		if err := rows.Scan(&aliasID, &canonicalID); err != nil {
			return nil, err
		}
		resolved[strings.TrimSpace(aliasID)] = strings.TrimSpace(canonicalID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return resolved, nil
}

func (db *Store) GetObjectsByChecksums(ctx context.Context, checksums []string) (map[string][]drs.DrsObject, error) {
	if len(checksums) == 0 {
		return nil, nil
	}
	objectsByID, err := db.fetchObjectsByIDsOrChecksums(ctx, nil, checksums)
	if err != nil {
		return nil, err
	}
	index := make(map[string][]drs.DrsObject, len(objectsByID)*2)
	for _, obj := range objectsByID {
		index[obj.Id] = append(index[obj.Id], *obj)
		for _, cs := range obj.Checksums {
			value := strings.TrimSpace(cs.Checksum)
			if value == "" {
				continue
			}
			index[value] = append(index[value], *obj)
			if objects.NormalizeChecksumType(cs.Type) == "sha256" {
				if normalized := objects.NormalizeOID(value); normalized != "" {
					index[normalized] = append(index[normalized], *obj)
				}
			}
		}
	}
	result := make(map[string][]drs.DrsObject, len(checksums))
	for _, cs := range checksums {
		requested := strings.TrimSpace(cs)
		if requested == "" {
			continue
		}
		lookup := requested
		if normalized := objects.NormalizeOID(requested); normalized != "" {
			lookup = normalized
		}
		if objs := index[lookup]; len(objs) > 0 {
			result[requested] = uniqueObjectsByID(objs)
		}
	}
	return result, nil
}

func (db *Store) ListScopedObjectIDsByChecksums(ctx context.Context, organization, project string, checksums []string) (map[string][]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" || project == "" || len(checksums) == 0 {
		return map[string][]string{}, nil
	}
	resource, err := clientaccess.ResourcePath(organization, project)
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
	args := make([]any, 0, len(normalized)+2)
	args = append(args, resource, "sha256")
	checksumCondition, checksumArgs := db.dialect.ListArgs("replace(lower(trim(c.checksum)), 'sha256:', '')", normalized)
	args = append(args, checksumArgs...)
	rows, err := db.queryContext(ctx, fmt.Sprintf(`
		SELECT DISTINCT replace(lower(trim(c.checksum)), 'sha256:', ''), c.object_id
		FROM drs_object_checksum c
		INNER JOIN drs_object_controlled_access ca ON ca.object_id = c.object_id
		WHERE ca.resource = ? AND replace(lower(trim(c.type)), '-', '') = ?
		  AND %s
		ORDER BY 1, 2`, checksumCondition), args...)
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

func (db *Store) ListObjectIDsByScope(ctx context.Context, organization, project string) ([]string, error) {
	organization = strings.TrimSpace(organization)
	project = strings.TrimSpace(project)
	if organization == "" {
		rows, err := db.queryContext(ctx, `SELECT id FROM drs_object ORDER BY id`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		return scanObjectIDs(rows)
	}

	var (
		rows *sql.Rows
		err  error
	)
	if project != "" {
		resource, resourceErr := clientaccess.ResourcePath(organization, project)
		if resourceErr != nil {
			return nil, resourceErr
		}
		rows, err = db.queryContext(ctx, `
			SELECT DISTINCT ca.object_id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
			WHERE ca.resource = ?
			ORDER BY ca.object_id`, resource)
	} else {
		condition, scopeArgs, scopeErr := scopeResourceCondition("ca.resource", organization, "")
		if scopeErr != nil {
			return nil, scopeErr
		}
		rows, err = db.queryContext(ctx, `
			SELECT DISTINCT ca.object_id
			FROM drs_object_controlled_access ca
			INNER JOIN drs_object o ON o.id = ca.object_id
			WHERE `+condition+`
			ORDER BY ca.object_id`, scopeArgs...)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *Store) ListObjectIDsByResources(ctx context.Context, resources []string, includeUnscoped bool) ([]string, error) {
	resources = clientaccess.NormalizeAccessResources(resources)
	if len(resources) == 0 && !includeUnscoped {
		return []string{}, nil
	}

	args := make([]any, 0, len(resources))
	parts := make([]string, 0, 2)
	if len(resources) > 0 {
		resourceCondition, resourceArgs := db.dialect.ListArgs("ca.resource", resources)
		parts = append(parts, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id AND `+resourceCondition+`
		)`)
		args = append(args, resourceArgs...)
	}
	if includeUnscoped {
		parts = append(parts, `NOT EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca
			WHERE ca.object_id = o.id
		)`)
	}

	rows, err := db.queryContext(ctx, `
		SELECT DISTINCT o.id
		FROM drs_object o
		WHERE `+strings.Join(parts, " OR ")+`
		ORDER BY o.id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *Store) ListObjectIDsPage(ctx context.Context, query objects.ObjectIDPageQuery) ([]string, error) {
	if strings.TrimSpace(query.ObjectURL) != "" {
		return db.listObjectIDsPageByURL(ctx, query)
	}
	return db.listObjectIDsPageByScope(ctx, query)
}

func (db *Store) listObjectIDsPageByScope(ctx context.Context, query objects.ObjectIDPageQuery) ([]string, error) {
	query.Scope.Organization = strings.TrimSpace(query.Scope.Organization)
	query.Scope.Project = strings.TrimSpace(query.Scope.Project)
	query.StartAfter = strings.TrimSpace(query.StartAfter)
	if query.Limit <= 0 {
		return []string{}, nil
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	args := make([]any, 0, 8)
	conditions := make([]string, 0, 4)
	if query.Scope.Organization != "" {
		resourceCondition, resourceArgs, err := scopeResourceCondition("ca_scope.resource", query.Scope.Organization, query.Scope.Project)
		if err != nil {
			return nil, err
		}
		args = append(args, resourceArgs...)
		conditions = append(conditions, `EXISTS (SELECT 1 FROM drs_object_controlled_access ca_scope WHERE ca_scope.object_id = o.id AND `+resourceCondition+`)`)
	}
	if query.RestrictToVisibleResources {
		resources := clientaccess.NormalizeAccessResources(query.VisibleResources)
		parts := make([]string, 0, 2)
		if len(resources) > 0 {
			resourceCondition, resourceArgs := db.dialect.ListArgs("ca_auth.resource", resources)
			parts = append(parts, `EXISTS (SELECT 1 FROM drs_object_controlled_access ca_auth WHERE ca_auth.object_id = o.id AND `+resourceCondition+`)`)
			args = append(args, resourceArgs...)
		}
		if query.IncludeUnscoped {
			parts = append(parts, `NOT EXISTS (SELECT 1 FROM drs_object_controlled_access ca_auth WHERE ca_auth.object_id = o.id)`)
		}
		if len(parts) == 0 {
			return []string{}, nil
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}
	if query.StartAfter != "" {
		args = append(args, query.StartAfter)
		conditions = append(conditions, "o.id > ?")
	}
	statement := `SELECT DISTINCT o.id FROM drs_object o`
	if len(conditions) > 0 {
		statement += " WHERE " + strings.Join(conditions, " AND ")
	}
	statement += " ORDER BY o.id LIMIT ? OFFSET ?"
	args = append(args, query.Limit, query.Offset)
	rows, err := db.queryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func (db *Store) listObjectIDsPageByURL(ctx context.Context, query objects.ObjectIDPageQuery) ([]string, error) {
	query.ObjectURL = strings.TrimSpace(query.ObjectURL)
	query.Scope.Organization = strings.TrimSpace(query.Scope.Organization)
	query.Scope.Project = strings.TrimSpace(query.Scope.Project)
	query.StartAfter = strings.TrimSpace(query.StartAfter)
	if query.ObjectURL == "" || query.Limit <= 0 {
		return []string{}, nil
	}
	if query.Offset < 0 {
		query.Offset = 0
	}

	args := []any{query.ObjectURL}
	conditions := []string{"am.url = ?"}
	if query.Scope.Organization != "" {
		scopeCondition, scopeArgs, err := scopeResourceCondition("ca_scope.resource", query.Scope.Organization, query.Scope.Project)
		if err != nil {
			return nil, err
		}
		args = append(args, scopeArgs...)
		conditions = append(conditions, `EXISTS (
			SELECT 1
			FROM drs_object_controlled_access ca_scope
			WHERE ca_scope.object_id = o.id AND `+scopeCondition+`
		)`)
	}
	if query.RestrictToVisibleResources {
		resources := clientaccess.NormalizeAccessResources(query.VisibleResources)
		if len(resources) == 0 && !query.IncludeUnscoped {
			return []string{}, nil
		}
		parts := make([]string, 0, 2)
		if len(resources) > 0 {
			resourceCondition, resourceArgs := db.dialect.ListArgs("ca_auth.resource", resources)
			parts = append(parts, `EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id AND `+resourceCondition+`
			)`)
			args = append(args, resourceArgs...)
		}
		if query.IncludeUnscoped {
			parts = append(parts, `NOT EXISTS (
				SELECT 1
				FROM drs_object_controlled_access ca_auth
				WHERE ca_auth.object_id = o.id
			)`)
		}
		conditions = append(conditions, "("+strings.Join(parts, " OR ")+")")
	}
	if query.StartAfter != "" {
		args = append(args, query.StartAfter)
		conditions = append(conditions, "o.id > ?")
	}

	statement := `
		SELECT DISTINCT o.id
		FROM drs_object o
		INNER JOIN drs_object_access_method am ON am.object_id = o.id
		WHERE ` + strings.Join(conditions, " AND ") + `
		ORDER BY o.id
		LIMIT ? OFFSET ?`
	args = append(args, query.Limit, query.Offset)
	rows, err := db.queryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanObjectIDs(rows)
}

func scopeResourceCondition(column, organization, project string) (string, []any, error) {
	resource, err := clientaccess.ResourcePath(organization, project)
	if err != nil {
		return "", nil, err
	}
	if strings.TrimSpace(project) != "" {
		return column + " = ?", []any{resource}, nil
	}
	return "(" + column + " = ? OR " + column + " LIKE ? ESCAPE '\\')", []any{resource, likeEscape(resource+"/project/") + "%"}, nil
}

func likeEscape(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(value)
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
