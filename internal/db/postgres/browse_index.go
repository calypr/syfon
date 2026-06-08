package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	icommon "github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/lib/pq"
)

type browseIndexRow struct {
	ObjectID       string
	Resource       string
	NormalizedPath string
	ParentPath     string
	EntryName      string
}

func postgresBrowseRowsForObject(objectID, objectName string, resources []string) []browseIndexRow {
	info, ok, err := icommon.BrowsePathInfoFromName(objectName)
	if err != nil || !ok {
		return nil
	}
	normalizedResources := sycommon.NormalizeAccessResources(resources)
	rows := make([]browseIndexRow, 0, len(normalizedResources))
	for _, resource := range normalizedResources {
		rows = append(rows, browseIndexRow{
			ObjectID:       objectID,
			Resource:       resource,
			NormalizedPath: info.Normalized,
			ParentPath:     info.ParentPath,
			EntryName:      info.EntryName,
		})
	}
	return rows
}

func postgresDeleteBrowseRowsByIDsTx(ctx context.Context, tx *sql.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM drs_object_browse_index WHERE object_id = ANY($1)`, pq.Array(ids))
	return err
}

func postgresDeleteBrowseRowTx(ctx context.Context, tx *sql.Tx, objectID, resource string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM drs_object_browse_index WHERE object_id = $1 AND resource = $2`, objectID, resource)
	return err
}

func postgresInsertBrowseRowsTx(ctx context.Context, tx *sql.Tx, rows []browseIndexRow) error {
	if len(rows) == 0 {
		return nil
	}
	objectIDs := make([]string, 0, len(rows))
	resources := make([]string, 0, len(rows))
	normalizedPaths := make([]string, 0, len(rows))
	parentPaths := make([]string, 0, len(rows))
	entryNames := make([]string, 0, len(rows))
	for _, row := range rows {
		objectIDs = append(objectIDs, row.ObjectID)
		resources = append(resources, row.Resource)
		normalizedPaths = append(normalizedPaths, row.NormalizedPath)
		parentPaths = append(parentPaths, row.ParentPath)
		entryNames = append(entryNames, row.EntryName)
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO drs_object_browse_index (object_id, resource, normalized_path, parent_path, entry_name)
		SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[])
		ON CONFLICT (resource, object_id) DO UPDATE SET
			normalized_path = EXCLUDED.normalized_path,
			parent_path = EXCLUDED.parent_path,
			entry_name = EXCLUDED.entry_name`,
		pq.Array(objectIDs), pq.Array(resources), pq.Array(normalizedPaths), pq.Array(parentPaths), pq.Array(entryNames),
	)
	return err
}

func postgresRebuildBrowseRowsForObjectTx(ctx context.Context, tx *sql.Tx, objectID, objectName string, resources []string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_browse_index WHERE object_id = $1`, objectID); err != nil {
		return err
	}
	return postgresInsertBrowseRowsTx(ctx, tx, postgresBrowseRowsForObject(objectID, objectName, resources))
}

func (db *PostgresDB) ListObjectIDsPageByPath(ctx context.Context, organization, project, path, startAfter string, limit, offset int) ([]string, []models.BrowseDirectory, error) {
	resource, err := sycommon.ResourcePath(organization, project)
	if err != nil {
		return nil, nil, err
	}
	normalizedPath, _, err := icommon.NormalizeBrowsePath(path)
	if err != nil {
		return nil, nil, err
	}
	if offset < 0 {
		offset = 0
	}

	args := []any{resource, normalizedPath}
	query := `
		SELECT object_id
		FROM drs_object_browse_index
		WHERE resource = $1 AND parent_path = $2`
	if strings.TrimSpace(startAfter) != "" {
		args = append(args, strings.TrimSpace(startAfter))
		query += fmt.Sprintf(" AND object_id > $%d", len(args))
	}
	args = append(args, limit, offset)
	query += fmt.Sprintf(" ORDER BY object_id LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ids, err := scanObjectIDs(rows)
	if err != nil {
		return nil, nil, err
	}

	dirQuery := `SELECT normalized_path FROM drs_object_browse_index WHERE resource = $1 AND parent_path <> ''`
	dirArgs := []any{resource}
	if normalizedPath != "" {
		dirQuery = `SELECT normalized_path FROM drs_object_browse_index WHERE resource = $1 AND normalized_path LIKE $2 AND parent_path <> $3`
		dirArgs = append(dirArgs, normalizedPath+"/%", normalizedPath)
	}
	dirRows, err := db.db.QueryContext(ctx, dirQuery+" ORDER BY normalized_path", dirArgs...)
	if err != nil {
		return nil, nil, err
	}
	defer dirRows.Close()

	directoriesByPath := make(map[string]models.BrowseDirectory)
	for dirRows.Next() {
		var candidatePath string
		if err := dirRows.Scan(&candidatePath); err != nil {
			return nil, nil, err
		}
		dirInfo, ok := icommon.ImmediateBrowseDirectory(normalizedPath, candidatePath)
		if !ok {
			continue
		}
		directoriesByPath[dirInfo.Normalized] = models.BrowseDirectory{Name: dirInfo.EntryName, Path: dirInfo.Normalized}
	}
	if err := dirRows.Err(); err != nil {
		return nil, nil, err
	}
	directories := make([]models.BrowseDirectory, 0, len(directoriesByPath))
	for _, directory := range directoriesByPath {
		directories = append(directories, directory)
	}
	sort.Slice(directories, func(i, j int) bool {
		if directories[i].Name == directories[j].Name {
			return directories[i].Path < directories[j].Path
		}
		return directories[i].Name < directories[j].Name
	})
	return ids, directories, nil
}
