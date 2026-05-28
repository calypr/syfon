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

func (db *SqliteDB) GetS3Credential(ctx context.Context, credentialID string) (*models.S3Credential, error) {
	var c models.S3Credential
	err := db.db.QueryRowContext(ctx, `
		SELECT credential_id, bucket, provider, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE credential_id = ?`, credentialID).Scan(
		&c.CredentialID, &c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint,
	)
	if err == sql.ErrNoRows {
		fallback, fallbackErr := db.getS3CredentialByPhysicalBucket(ctx, credentialID)
		if fallbackErr == nil {
			common.AuditS3CredentialAccess(ctx, "read", credentialID, nil)
			return fallback, nil
		}
		common.AuditS3CredentialAccess(ctx, "read", credentialID, fallbackErr)
		return nil, fallbackErr
	}
	if err != nil {
		wrapped := fmt.Errorf("failed to fetch credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "read", credentialID, wrapped)
		return nil, wrapped
	}
	parsed, err := crypto.ParseS3CredentialFromStorage(&c)
	if err != nil {
		wrapped := fmt.Errorf("failed to decrypt credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "read", credentialID, wrapped)
		return nil, wrapped
	}
	common.AuditS3CredentialAccess(ctx, "read", credentialID, nil)
	return parsed, nil
}

func (db *SqliteDB) getS3CredentialByPhysicalBucket(ctx context.Context, bucket string) (*models.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, `
		SELECT credential_id, bucket, provider, region, access_key, secret_key, endpoint
		FROM s3_credential WHERE bucket = ?`, bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch credential by bucket: %w", err)
	}
	defer rows.Close()

	matches := make([]models.S3Credential, 0, 2)
	for rows.Next() {
		var c models.S3Credential
		if err := rows.Scan(&c.CredentialID, &c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint); err != nil {
			return nil, err
		}
		matches = append(matches, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	switch len(matches) {
	case 0:
		return nil, fmt.Errorf("credential not found")
	case 1:
		parsed, err := crypto.ParseS3CredentialFromStorage(&matches[0])
		if err != nil {
			return nil, fmt.Errorf("failed to decrypt credential: %w", err)
		}
		return parsed, nil
	default:
		return nil, fmt.Errorf("multiple credentials use physical bucket %q; define the scope inline with the intended bucket credential", bucket)
	}
}

func (db *SqliteDB) SaveS3Credential(ctx context.Context, cred *models.S3Credential) error {
	bucket := ""
	if cred != nil {
		bucket = cred.Bucket
		if strings.TrimSpace(cred.CredentialID) == "" {
			cred.CredentialID = common.DeriveCredentialID(cred.Bucket, cred.Provider, cred.Region, cred.Endpoint, cred.AccessKey)
		}
	}
	stored, err := crypto.PrepareS3CredentialForStorage(cred)
	if err != nil {
		wrapped := fmt.Errorf("failed to prepare credential for storage: %w", err)
		common.AuditS3CredentialAccess(ctx, "write", bucket, wrapped)
		return wrapped
	}

	// SQLite UPSERT syntax: INSERT INTO ... ON CONFLICT (...) DO UPDATE SET ...
	_, err = db.db.ExecContext(ctx, `
		INSERT INTO s3_credential (credential_id, bucket, provider, region, access_key, secret_key, endpoint)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (credential_id) DO UPDATE SET
			bucket = excluded.bucket,
			provider = excluded.provider,
			region = excluded.region,
			access_key = excluded.access_key,
			secret_key = excluded.secret_key,
			endpoint = excluded.endpoint`,
		stored.CredentialID, stored.Bucket, strings.ToLower(strings.TrimSpace(defaultProvider(stored.Provider))), stored.Region, stored.AccessKey, stored.SecretKey, stored.Endpoint,
	)
	if err != nil {
		wrapped := fmt.Errorf("failed to save credential: %w", err)
		common.AuditS3CredentialAccess(ctx, "write", stored.Bucket, wrapped)
		return wrapped
	}
	common.AuditS3CredentialAccess(ctx, "write", stored.Bucket, nil)
	return nil
}

func (db *SqliteDB) DeleteS3Credential(ctx context.Context, credentialID string) error {
	resolvedID, err := db.resolveCredentialID(ctx, credentialID)
	if err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", credentialID, err)
		return err
	}
	if _, err := db.db.ExecContext(ctx, "DELETE FROM bucket_scope WHERE credential_id = ?", resolvedID); err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", credentialID, err)
		return fmt.Errorf("failed to delete bucket scopes for %s: %w", credentialID, err)
	}
	res, err := db.db.ExecContext(ctx, "DELETE FROM s3_credential WHERE credential_id = ?", resolvedID)
	if err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", credentialID, err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		common.AuditS3CredentialAccess(ctx, "delete", credentialID, err)
		return err
	}
	if rows == 0 {
		notFoundErr := fmt.Errorf("credential not found")
		common.AuditS3CredentialAccess(ctx, "delete", credentialID, notFoundErr)
		return notFoundErr
	}
	common.AuditS3CredentialAccess(ctx, "delete", credentialID, nil)
	return nil
}

func (db *SqliteDB) resolveCredentialID(ctx context.Context, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("credential not found")
	}
	var exact string
	err := db.db.QueryRowContext(ctx, "SELECT credential_id FROM s3_credential WHERE credential_id = ?", raw).Scan(&exact)
	if err == nil {
		return exact, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	cred, err := db.getS3CredentialByPhysicalBucket(ctx, raw)
	if err != nil {
		return "", err
	}
	return cred.CredentialID, nil
}

func (db *SqliteDB) ListS3Credentials(ctx context.Context) ([]models.S3Credential, error) {
	rows, err := db.db.QueryContext(ctx, "SELECT credential_id, bucket, provider, region, access_key, secret_key, endpoint FROM s3_credential")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var creds []models.S3Credential
	for rows.Next() {
		var c models.S3Credential
		if err := rows.Scan(&c.CredentialID, &c.Bucket, &c.Provider, &c.Region, &c.AccessKey, &c.SecretKey, &c.Endpoint); err != nil {
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
	credentialID := strings.TrimSpace(scope.CredentialID)
	bucket := strings.TrimSpace(scope.Bucket)
	prefix := strings.Trim(strings.TrimSpace(scope.PathPrefix), "/")
	if credentialID == "" {
		credentialID = bucket
	}
	if org == "" || bucket == "" {
		return fmt.Errorf("organization and bucket are required")
	}

	existing, err := db.GetBucketScope(ctx, org, project)
	if err != nil && !errors.Is(err, common.ErrNotFound) {
		return err
	}
	if err == nil && existing != nil {
		if strings.EqualFold(strings.TrimSpace(existing.CredentialID), credentialID) && strings.EqualFold(strings.TrimSpace(existing.Bucket), bucket) && strings.Trim(strings.TrimSpace(existing.PathPrefix), "/") == prefix {
			return nil
		}
		_, err = db.db.ExecContext(ctx, `
			UPDATE bucket_scope
			SET credential_id = ?, bucket = ?, path_prefix = ?
			WHERE organization = ? AND project_id = ?
		`, credentialID, bucket, prefix, org, project)
		if err != nil {
			return fmt.Errorf("failed to update bucket scope: %w", err)
		}
		return nil
	}

	_, err = db.db.ExecContext(ctx, `
		INSERT INTO bucket_scope (organization, project_id, credential_id, bucket, path_prefix)
		VALUES (?, ?, ?, ?, ?)
	`, org, project, credentialID, bucket, prefix)
	if err != nil {
		return fmt.Errorf("failed to create bucket scope: %w", err)
	}
	return nil
}

func (db *SqliteDB) GetBucketScope(ctx context.Context, organization, projectID string) (*models.BucketScope, error) {
	var s models.BucketScope
	err := db.db.QueryRowContext(ctx, `
		SELECT organization, project_id, credential_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
		WHERE organization = ? AND project_id = ?
	`, strings.TrimSpace(organization), strings.TrimSpace(projectID)).Scan(
		&s.Organization, &s.ProjectID, &s.CredentialID, &s.Bucket, &s.PathPrefix,
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
		SELECT organization, project_id, credential_id, bucket, COALESCE(path_prefix, '')
		FROM bucket_scope
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []models.BucketScope
	for rows.Next() {
		var s models.BucketScope
		if err := rows.Scan(&s.Organization, &s.ProjectID, &s.CredentialID, &s.Bucket, &s.PathPrefix); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}
