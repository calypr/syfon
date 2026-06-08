package sqlite

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	sycommon "github.com/calypr/syfon/common"
	icommon "github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

type browseIndexRow struct {
	ObjectID       string
	Resource       string
	NormalizedPath string
	ParentPath     string
	EntryName      string
}

func sqliteBrowseRowsForObject(objectID, objectName string, resources []string) []browseIndexRow {
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

func sqliteDeleteBrowseRowsByIDsTx(tx *sql.Tx, ids []string) error {
	return execSQLiteDeleteByIDs(tx, "drs_object_browse_index", ids)
}

func sqliteInsertBrowseRowsTx(tx *sql.Tx, rows []browseIndexRow) error {
	if len(rows) == 0 {
		return nil
	}
	args := make([]interface{}, 0, len(rows)*5)
	for _, row := range rows {
		args = append(args, row.ObjectID, row.Resource, row.NormalizedPath, row.ParentPath, row.EntryName)
	}
	return execSQLiteBulkInsert(
		tx,
		"INSERT INTO drs_object_browse_index (object_id, resource, normalized_path, parent_path, entry_name) VALUES ",
		"(?, ?, ?, ?, ?)",
		5,
		args,
		" ON CONFLICT(resource, object_id) DO UPDATE SET normalized_path=excluded.normalized_path, parent_path=excluded.parent_path, entry_name=excluded.entry_name",
	)
}

func sqliteRebuildBrowseRowsForObjectTx(ctx context.Context, tx *sql.Tx, objectID, objectName string, resources []string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM drs_object_browse_index WHERE object_id = ?`, objectID); err != nil {
		return err
	}
	return sqliteInsertBrowseRowsTx(tx, sqliteBrowseRowsForObject(objectID, objectName, resources))
}

func (db *SqliteDB) ListObjectIDsPageByPath(ctx context.Context, organization, project, path, startAfter string, limit, offset int) ([]string, []models.BrowseDirectory, error) {
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
		WHERE resource = ? AND parent_path = ?`
	if strings.TrimSpace(startAfter) != "" {
		args = append(args, strings.TrimSpace(startAfter))
		query += " AND object_id > ?"
	}
	args = append(args, limit, offset)
	query += " ORDER BY object_id LIMIT ? OFFSET ?"
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	ids, err := scanObjectIDs(rows)
	if err != nil {
		return nil, nil, err
	}

	dirQuery := `SELECT normalized_path FROM drs_object_browse_index WHERE resource = ? AND parent_path <> '' ORDER BY normalized_path`
	dirArgs := []any{resource}
	if normalizedPath != "" {
		dirQuery = `SELECT normalized_path FROM drs_object_browse_index WHERE resource = ? AND normalized_path LIKE ? AND parent_path <> ? ORDER BY normalized_path`
		dirArgs = append(dirArgs, normalizedPath+"/%", normalizedPath)
	}
	dirRows, err := db.db.QueryContext(ctx, dirQuery, dirArgs...)
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
