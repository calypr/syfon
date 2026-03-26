package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/calypr/drs-server/db/core"
)

func (db *SqliteDB) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	var c core.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT bucket, provider, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE bucket = ?`, bucket).Scan(
		&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("credential not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to fetch credential: %w", err)
	}
	return &c, nil
}

func (db *SqliteDB) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	// SQLite UPSERT syntax: INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
	_, err := db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (bucket, provider, region, access_key, secret_key, endpoint)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket) DO UPDATE SET
			provider = excluded.provider,
			region = excluded.region,
			access_key = excluded.access_key,
			secret_key = excluded.secret_key,
			endpoint = excluded.endpoint`,
		cred.Bucket, strings.ToLower(strings.TrimSpace(defaultProvider(cred.Provider))), cred.Region, cred.AccessKey, cred.SecretKey, cred.Endpoint,
	)
	if err != nil {
		return fmt.Errorf("failed to save credential: %w", err)
	}
	return nil
}

func (db *SqliteDB) DeleteS3Credential(ctx context.Context, bucket string) error {
	_, _ = db.db.ExecContext(ctx, "DELETE FROM bucket_scope WHERE bucket = ?", bucket)
	res, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE bucket = ?", bucket)
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

func (db *SqliteDB) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT bucket, provider, region, access_key, secret_key, endpoint FROM s3_credential")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []core.S3Credential
	for rows.Next() {
		var c core.S3Credential
		if err := rows.Scan(&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint); err != nil {
			return nil, err
		}
		creds = append(creds, c)
	}
	return creds, nil
}

func (db *SqliteDB) CreateBucketScope(ctx context.Context, scope *core.BucketScope) error {
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
		VALUES (?, ?, ?, ?)
	`, org, project, bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to create bucket scope: %w", err)
	}
	return nil
}

func (db *SqliteDB) GetBucketScope(ctx context.Context, organization, projectID string) (*core.BucketScope, error) {
	var s core.BucketScope
	err := db.db.QueryRowContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = ? AND project_id = ?
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

func (db *SqliteDB) ListBucketScopes(ctx context.Context) ([]core.BucketScope, error) {
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
