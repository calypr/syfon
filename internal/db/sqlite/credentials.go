package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"strings"

	"github.com/calypr/syfon/internal/crypto"
)

func (db *SqliteDB) GetS3Credential(ctx context.Context, bucket string) (*models.S3Credential, error) {
	var c models.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT bucket, provider, region, access_key, secret_key, endpoint,
		       COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '')
		FROM s3_credential WHERE bucket = ?`, bucket).Scan(
		&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint,
		&c.BillingLogBucket, &c.BillingLogPrefix,
	)
	if err == sql.ErrNoRows {
		notFoundErr := fmt.Errorf("credential not found")
		common.AuditS3CredentialAccess(ctx, "read", bucket, notFoundErr)
		return nil, notFoundErr
	}
	if err != nil {
		wrapped := fmt.Errorf("failed to fetch credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "read", bucket, wrapped)
		return nil, wrapped
	}
	parsed, err := crypto.ParseS3CredentialFromStorage(&c)
	if err != nil {
		wrapped := fmt.Errorf("failed to decrypt credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "read", bucket, wrapped)
		return nil, wrapped
	}
	common.AuditS3CredentialAccess(ctx, "read", bucket, nil)
	return parsed, nil
}

func (db *SqliteDB) SaveS3Credential(ctx context.Context, cred *models.S3Credential) error {
	bucket := ""
	if cred != nil {
		bucket = cred.Bucket
	}
	stored, err := crypto.PrepareS3CredentialForStorage(cred)
	if err != nil {
		wrapped := fmt.Errorf("failed to prepare credential for storage: %w", err)
		common.AuditS3CredentialAccess(ctx, "write", bucket, wrapped)
		return wrapped
	}

	// SQLite UPSERT syntax: INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
	_, err = db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (bucket, provider, region, access_key, secret_key, endpoint, billing_log_bucket, billing_log_prefix)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (bucket) DO UPDATE SET
			provider = excluded.provider,
			region = excluded.region,
			access_key = excluded.access_key,
			secret_key = excluded.secret_key,
			endpoint = excluded.endpoint,
			billing_log_bucket = excluded.billing_log_bucket,
			billing_log_prefix = excluded.billing_log_prefix`,
		stored.Bucket, strings.ToLower(strings.TrimSpace(defaultProvider(stored.Provider))), stored.Region, stored.AccessKey, stored.SecretKey, stored.Endpoint,
		stored.BillingLogBucket, strings.Trim(strings.TrimSpace(stored.BillingLogPrefix), "/"),
	)
	if err != nil {
		wrapped := fmt.Errorf("failed to save credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "write", stored.Bucket, wrapped)
		return wrapped
	}
	common.AuditS3CredentialAccess(ctx, "write", stored.Bucket, nil)
	return nil
}

func (db *SqliteDB) DeleteS3Credential(ctx context.Context, bucket string) error {
	_, _ = db.db.ExecContext(ctx, "DELETE FROM bucket_scope WHERE bucket = ?", bucket)
	res, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE bucket = ?", bucket)
	if err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", bucket, err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", bucket, err)
		return err
	}
	if rows == 0 {
		notFoundErr := fmt.Errorf("credential not found")
		common.AuditS3CredentialAccess(ctx, "delete", bucket, notFoundErr)
		return notFoundErr
	}
	common.AuditS3CredentialAccess(ctx, "delete", bucket, nil)
	return nil
}

func (db *SqliteDB) ListS3Credentials(ctx context.Context) ([]models.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT bucket, provider, region, access_key, secret_key, endpoint, COALESCE(billing_log_bucket, ''), COALESCE(billing_log_prefix, '') FROM s3_credential")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []models.S3Credential
	for rows.Next() {
		var c models.S3Credential
		if err := rows.Scan(&c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint, &c.BillingLogBucket, &c.BillingLogPrefix); err != nil {
			common.AuditS3CredentialAccess(ctx, "list", "", err)
			return nil, err
		}
		parsed, err := crypto.ParseS3CredentialFromStorage(&c)
		if err != nil {
			wrapped := fmt.Errorf("failed to decrypt credential for bucket %s: %w", c.Bucket, err)
			common.AuditS3CredentialAccess(ctx, "list", c.Bucket, wrapped)
			return nil, wrapped
		}
		creds = append(creds, *parsed)
	}
	common.AuditS3CredentialAccess(ctx, "list", "", nil)
	return creds, nil
}

func (db *SqliteDB) CreateBucketScope(ctx context.Context, scope *models.BucketScope) error {
	if scope == nil {
		return fmt.Errorf("scope is required")
	}
	org := strings.TrimSpace(scope.Organization)
	project := strings.TrimSpace(scope.ProjectID)
	bucket := strings.TrimSpace(scope.Bucket)
	prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
	if org == "" || bucket == "" {
		return fmt.Errorf("organization and bucket are required")
	}

	existing, err := db.GetBucketScope(ctx, org, project)
	if err != nil && !errors.Is(err, common.ErrNotFound) {
		return err
	}
	if err == nil && existing != nil {
		if strings.EqualFold(strings.TrimSpace(existing.Bucket), bucket) && strings.Trim(strings.TrimSpace(existing.PathPrefix), "/") == prefix {
			return nil
		}
		return fmt.Errorf("%w: scope already assigned to bucket=%s prefix=%s", common.ErrConflict, existing.Bucket, existing.PathPrefix)
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

func (db *SqliteDB) GetBucketScope(ctx context.Context, organization, projectID string) (*models.BucketScope, error) {
	var s models.BucketScope
	err := db.db.QueryRowContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = ? AND project_id = ?
	`, strings.TrimSpace(organization), strings.TrimSpace(projectID)).Scan(
		&s.Organization, &s.ProjectID, &s.Bucket, &s.PathPrefix,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: bucket scope not found", common.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket scope: %w", err)
	}
	return &s, nil
}

func (db *SqliteDB) ListBucketScopes(ctx context.Context) ([]models.BucketScope, error) {
	rows, err := db.db.QueryContext(ctx, `
		SELECT organization, project_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.BucketScope
	for rows.Next() {
		var s models.BucketScope
		if err := rows.Scan(&s.Organization, &s.ProjectID, &s.Bucket, &s.PathPrefix); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
