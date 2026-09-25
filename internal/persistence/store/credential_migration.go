package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/calypr/syfon/internal/buckets"
)

// encryptLegacyCredentials replaces plaintext credential fields before the
// store is exposed to callers. The transaction is safe to retry after a crash.
func (s *Store) encryptLegacyCredentials(ctx context.Context) error {
	if s.cipher == nil {
		return nil
	}
	return s.withContentWrite(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT credential_id, COALESCE(access_key, ''), COALESCE(secret_key, '') FROM s3_credential`)
		if err != nil {
			return fmt.Errorf("list credential fields for encryption: %w", err)
		}
		var credentials []buckets.Credential
		for rows.Next() {
			var credential buckets.Credential
			if err := rows.Scan(&credential.CredentialID, &credential.AccessKey, &credential.SecretKey); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan credential fields for encryption: %w", err)
			}
			credentials = append(credentials, credential)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read credential fields for encryption: %w", err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if len(credentials) == 0 {
			return nil
		}
		enabled, err := s.cipher.Enabled()
		if err != nil {
			return fmt.Errorf("check credential encryption: %w", err)
		}
		if !enabled {
			return nil
		}
		for i := range credentials {
			credential := &credentials[i]
			stored, err := s.cipher.Prepare(ctx, credential)
			if err != nil {
				return fmt.Errorf("encrypt credential %q: %w", credential.CredentialID, err)
			}
			if stored.AccessKey == credential.AccessKey && stored.SecretKey == credential.SecretKey {
				continue
			}
			if _, err := s.execOn(ctx, tx, `UPDATE s3_credential SET access_key = ?, secret_key = ? WHERE credential_id = ?`,
				stored.AccessKey, stored.SecretKey, credential.CredentialID); err != nil {
				return fmt.Errorf("persist encrypted credential %q: %w", credential.CredentialID, err)
			}
		}
		return nil
	})
}
