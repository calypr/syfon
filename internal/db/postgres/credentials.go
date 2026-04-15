package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/calypr/syfon/internal/db/core"
)

func (db *PostgresDB) GetS3Credential(ctx context.Context, bucket string) (*core.S3Credential, error) {
	var c core.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT bucket, provider, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE bucket = $1`, bucket).Scan(
		&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint,
	)
	if err == sql.ErrNoRows {
		notFoundErr := fmt.Errorf("credential not found")
		core.AuditS3CredentialAccess(ctx, "read", bucket, notFoundErr)
		return nil, notFoundErr
	}
	if err != nil {
		wrapped := fmt.Errorf("failed to fetch credential: %w", err)
		core.AuditS3CredentialAccess(ctx, "read", bucket, wrapped)
		return nil, wrapped
	}
	parsed, err := core.ParseS3CredentialFromStorage(&c)
	if err != nil {
		wrapped := fmt.Errorf("failed to decrypt credential: %w", err)
		core.AuditS3CredentialAccess(ctx, "read", bucket, wrapped)
		return nil, wrapped
	}
	core.AuditS3CredentialAccess(ctx, "read", bucket, nil)
	return parsed, nil
}

func (db *PostgresDB) SaveS3Credential(ctx context.Context, cred *core.S3Credential) error {
	bucket := ""
	if cred != nil {
		bucket = cred.Bucket
	}
	stored, err := core.PrepareS3CredentialForStorage(cred)
	if err != nil {
		wrapped := fmt.Errorf("failed to prepare credential for storage: %w", err)
		core.AuditS3CredentialAccess(ctx, "write", bucket, wrapped)
		return wrapped
	}

	_, err = db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (bucket, provider, region, access_key, secret_key, endpoint)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (bucket) DO UPDATE SET
			provider = EXCLUDED.provider,
			region = EXCLUDED.region,
			access_key = EXCLUDED.access_key,
			secret_key = EXCLUDED.secret_key,
			endpoint = EXCLUDED.endpoint`,
		stored.Bucket, strings.ToLower(strings.TrimSpace(defaultProvider(stored.Provider))), stored.Region, stored.AccessKey, stored.SecretKey, stored.Endpoint,
	)
	if err != nil {
		wrapped := fmt.Errorf("failed to save credential: %w", err)
		core.AuditS3CredentialAccess(ctx, "write", stored.Bucket, wrapped)
		return wrapped
	}
	core.AuditS3CredentialAccess(ctx, "write", stored.Bucket, nil)
	return nil
}

func (db *PostgresDB) DeleteS3Credential(ctx context.Context, bucket string) error {
	// 1. Delete bucket scopes first (cascade delete is on object_id, but bucket_scope is manual link)
	_, _ = db.db.ExecContext(ctx, "DELETE FROM bucket_scope WHERE bucket = $1", bucket)

	result, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE bucket = $1", bucket)
	if err != nil {
		core.AuditS3CredentialAccess(ctx, "delete", bucket, err)
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		core.AuditS3CredentialAccess(ctx, "delete", bucket, err)
		return err
	}
	if rows == 0 {
		notFoundErr := fmt.Errorf("credential not found")
		core.AuditS3CredentialAccess(ctx, "delete", bucket, notFoundErr)
		return notFoundErr
	}
	core.AuditS3CredentialAccess(ctx, "delete", bucket, nil)
	return nil
}

func (db *PostgresDB) ListS3Credentials(ctx context.Context) ([]core.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT bucket, provider, region, access_key, secret_key, endpoint FROM s3_credential")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []core.S3Credential
	for rows.Next() {
		var c core.S3Credential
		if err := rows.Scan(&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint); err != nil {
			core.AuditS3CredentialAccess(ctx, "list", "", err)
			return nil, err
		}
		parsed, err := core.ParseS3CredentialFromStorage(&c)
		if err != nil {
			wrapped := fmt.Errorf("failed to decrypt credential for bucket %s: %w", c.Bucket, err)
			core.AuditS3CredentialAccess(ctx, "list", c.Bucket, wrapped)
			return nil, wrapped
		}
		creds = append(creds, *parsed)
	}
	core.AuditS3CredentialAccess(ctx, "list", "", nil)
	return creds, nil
}

func (db *PostgresDB) CreateBucketScope(ctx context.Context, scope *core.BucketScope) error {
	if scope == nil {
		return fmt.Errorf("scope is required")
	}
	org := strings.TrimSpace(scope.Organization)
	project := strings.TrimSpace(scope.ProjectID)
	bucket := strings.TrimSpace(scope.Bucket)
	prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")

	if org == "" || project == "" || bucket == "" {
		return fmt.Errorf("organization, project_id, and bucket are required")
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
		VALUES ($1, $2, $3, $4)
	`, org, project, bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to create bucket scope: %w", err)
	}
	return nil
}

func (db *PostgresDB) GetBucketScope(ctx context.Context, organization, projectID string) (*core.BucketScope, error) {
	var s core.BucketScope
	err := db.db.QueryRowContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = $1 AND project_id = $2
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

func (db *PostgresDB) ListBucketScopes(ctx context.Context) ([]core.BucketScope, error) {
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
