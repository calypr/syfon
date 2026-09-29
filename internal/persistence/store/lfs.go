package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/internal/objects"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
)

const (
	lfsUploadReceiptJSONKey    = "_syfon_lfs_upload_receipt"
	lfsPendingExpiresAtJSONKey = "_syfon_lfs_pending_expires_at"
)

func (db *Store) SavePendingMetadata(ctx context.Context, entries []transferlfs.PendingMetadata) error {
	if len(entries) == 0 {
		return nil
	}
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := db.dialect.LockContentWrite(ctx, tx); err != nil {
		return err
	}

	if _, err := db.txExecContext(ctx, tx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= ?`, time.Now().UTC()); err != nil {
		return fmt.Errorf("failed to prune expired pending metadata: %w", err)
	}

	for _, e := range entries {
		var receipt *transferlfs.UploadReceipt
		var existingRaw string
		var existingExpiry time.Time
		lookupErr := db.txQueryRowContext(ctx, tx, `
			SELECT candidate_json, expires_time
			FROM lfs_pending_metadata
			WHERE oid = ?
		`, e.OID).Scan(&existingRaw, &existingExpiry)
		if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
			return fmt.Errorf("failed to load existing pending metadata for oid %s: %w", e.OID, lookupErr)
		}
		if lookupErr == nil {
			_, receipt, _, err = decodePendingMetadataJSON(existingRaw)
			if err != nil {
				return fmt.Errorf("failed to parse existing pending metadata for oid %s: %w", e.OID, err)
			}
		}
		expiresAt := e.ExpiresAt.UTC()
		if receipt != nil {
			if receipt.ExpiresAt.After(expiresAt) {
				expiresAt = receipt.ExpiresAt.UTC()
			}
			if existingExpiry.After(expiresAt) {
				expiresAt = existingExpiry.UTC()
			}
		}
		raw, err := encodePendingMetadataJSON(&e.Candidate, receipt, &expiresAt)
		if err != nil {
			return fmt.Errorf("failed to marshal pending candidate for oid %s: %w", e.OID, err)
		}
		if _, err := db.txExecContext(ctx, tx, `
			INSERT INTO lfs_pending_metadata (oid, candidate_json, created_time, expires_time)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (oid) DO UPDATE SET
				candidate_json = EXCLUDED.candidate_json,
				created_time = EXCLUDED.created_time,
				expires_time = EXCLUDED.expires_time
		`, e.OID, string(raw), e.CreatedAt.UTC(), expiresAt); err != nil {
			return fmt.Errorf("failed to save pending metadata for oid %s: %w", e.OID, err)
		}
	}
	return tx.Commit()
}

func (db *Store) GetPendingMetadata(ctx context.Context, oid string) (*transferlfs.PendingMetadata, error) {
	if _, err := db.execContext(ctx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= ?`, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("failed to prune expired pending metadata: %w", err)
	}

	var (
		raw       string
		createdAt time.Time
		expiresAt time.Time
	)
	if err := db.queryRowContext(ctx, `
		SELECT candidate_json, created_time, expires_time
		FROM lfs_pending_metadata
		WHERE oid = ? AND expires_time > ?
	`, oid, time.Now().UTC()).Scan(&raw, &createdAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: pending metadata not found", errorapi.ErrNotFound)
		}
		return nil, fmt.Errorf("failed to load pending metadata for oid %s: %w", oid, err)
	}

	fields, receipt, candidateExpiresAt, err := decodePendingMetadataJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse pending metadata candidate for oid %s: %w", oid, err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: pending metadata not found", errorapi.ErrNotFound)
	}
	if candidateExpiresAt == nil {
		candidateExpiresAt = &expiresAt
	}
	if !candidateExpiresAt.After(time.Now().UTC()) {
		return nil, fmt.Errorf("%w: pending metadata not found", errorapi.ErrNotFound)
	}
	candidateJSON, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("failed to encode pending metadata candidate for oid %s: %w", oid, err)
	}
	var c lfsapi.DrsObjectCandidate
	if err := json.Unmarshal(candidateJSON, &c); err != nil {
		return nil, fmt.Errorf("failed to decode pending metadata candidate for oid %s: %w", oid, err)
	}

	return &transferlfs.PendingMetadata{
		OID:           oid,
		Candidate:     c,
		UploadReceipt: receipt,
		CreatedAt:     createdAt,
		ExpiresAt:     *candidateExpiresAt,
	}, nil
}

func (db *Store) SaveLFSUploadReceipt(ctx context.Context, receipt transferlfs.UploadReceipt) error {
	if receipt.OID == "" || receipt.OID != receipt.SHA256 || receipt.Size < 0 || receipt.StorageURL == "" || receipt.CompletedAt.IsZero() || !receipt.ExpiresAt.After(receipt.CompletedAt) {
		return fmt.Errorf("invalid LFS upload receipt for oid %s", receipt.OID)
	}

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := db.dialect.LockContentWrite(ctx, tx); err != nil {
		return err
	}

	var (
		existingRaw string
		createdAt   = receipt.CompletedAt.UTC()
		expiresAt   = receipt.ExpiresAt.UTC()
	)
	lookupErr := db.txQueryRowContext(ctx, tx, `
		SELECT candidate_json, created_time, expires_time
		FROM lfs_pending_metadata
		WHERE oid = ?
	`, receipt.OID).Scan(&existingRaw, &createdAt, &expiresAt)
	var candidateFields map[string]json.RawMessage
	var candidateExpiresAt *time.Time
	if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
		return fmt.Errorf("failed to load pending metadata for upload receipt %s: %w", receipt.OID, lookupErr)
	}
	if lookupErr == nil {
		var existingReceipt *transferlfs.UploadReceipt
		candidateFields, existingReceipt, candidateExpiresAt, err = decodePendingMetadataJSON(existingRaw)
		if err != nil {
			return fmt.Errorf("failed to parse pending metadata for upload receipt %s: %w", receipt.OID, err)
		}
		if len(candidateFields) > 0 && candidateExpiresAt == nil {
			candidateExpiry := expiresAt
			candidateExpiresAt = &candidateExpiry
		}
		if existingReceipt != nil && existingReceipt.ExpiresAt.After(expiresAt) {
			expiresAt = existingReceipt.ExpiresAt.UTC()
		}
		if receipt.ExpiresAt.After(expiresAt) {
			expiresAt = receipt.ExpiresAt.UTC()
		}
		if candidateExpiresAt != nil && expiresAt.After(*candidateExpiresAt) {
			candidateExpiry := expiresAt.UTC()
			candidateExpiresAt = &candidateExpiry
		}
	} else {
		candidateFields = make(map[string]json.RawMessage)
	}
	receiptJSON, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("failed to marshal LFS upload receipt %s: %w", receipt.OID, err)
	}
	candidateFields[lfsUploadReceiptJSONKey] = receiptJSON
	if candidateExpiresAt != nil {
		candidateExpiresJSON, err := json.Marshal(candidateExpiresAt)
		if err != nil {
			return fmt.Errorf("failed to marshal pending metadata expiry for oid %s: %w", receipt.OID, err)
		}
		candidateFields[lfsPendingExpiresAtJSONKey] = candidateExpiresJSON
	}
	raw, err := json.Marshal(candidateFields)
	if err != nil {
		return fmt.Errorf("failed to encode LFS upload receipt %s: %w", receipt.OID, err)
	}
	if _, err := db.txExecContext(ctx, tx, `
		INSERT INTO lfs_pending_metadata (oid, candidate_json, created_time, expires_time)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (oid) DO UPDATE SET
			candidate_json = EXCLUDED.candidate_json,
			created_time = EXCLUDED.created_time,
			expires_time = EXCLUDED.expires_time
	`, receipt.OID, string(raw), createdAt.UTC(), expiresAt.UTC()); err != nil {
		return fmt.Errorf("failed to save LFS upload receipt %s: %w", receipt.OID, err)
	}
	return tx.Commit()
}

func (db *Store) GetLFSUploadReceipt(ctx context.Context, oid string) (*transferlfs.UploadReceipt, error) {
	if _, err := db.execContext(ctx, `DELETE FROM lfs_pending_metadata WHERE expires_time <= ?`, time.Now().UTC()); err != nil {
		return nil, fmt.Errorf("failed to prune expired LFS upload receipts: %w", err)
	}
	var raw string
	if err := db.queryRowContext(ctx, `
		SELECT candidate_json
		FROM lfs_pending_metadata
		WHERE oid = ? AND expires_time > ?
	`, oid, time.Now().UTC()).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: completed LFS upload not found", errorapi.ErrNotFound)
		}
		return nil, fmt.Errorf("failed to load LFS upload receipt for oid %s: %w", oid, err)
	}
	_, receipt, _, err := decodePendingMetadataJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LFS upload receipt for oid %s: %w", oid, err)
	}
	if receipt == nil || receipt.ExpiresAt.Before(time.Now().UTC()) {
		return nil, fmt.Errorf("%w: completed LFS upload not found", errorapi.ErrNotFound)
	}
	return receipt, nil
}

func encodePendingMetadataJSON(candidate *lfsapi.DrsObjectCandidate, receipt *transferlfs.UploadReceipt, candidateExpiresAt *time.Time) ([]byte, error) {
	fields := make(map[string]json.RawMessage)
	if candidate != nil {
		candidateJSON, err := json.Marshal(candidate)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(candidateJSON, &fields); err != nil {
			return nil, err
		}
	}
	if receipt != nil {
		receiptJSON, err := json.Marshal(receipt)
		if err != nil {
			return nil, err
		}
		fields[lfsUploadReceiptJSONKey] = receiptJSON
	}
	if candidate != nil && candidateExpiresAt != nil {
		expiresJSON, err := json.Marshal(candidateExpiresAt.UTC())
		if err != nil {
			return nil, err
		}
		fields[lfsPendingExpiresAtJSONKey] = expiresJSON
	}
	return json.Marshal(fields)
}

func decodePendingMetadataJSON(raw string) (map[string]json.RawMessage, *transferlfs.UploadReceipt, *time.Time, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return nil, nil, nil, err
	}
	if fields == nil {
		fields = make(map[string]json.RawMessage)
	}
	var receipt *transferlfs.UploadReceipt
	if receiptJSON, ok := fields[lfsUploadReceiptJSONKey]; ok {
		if err := json.Unmarshal(receiptJSON, &receipt); err != nil {
			return nil, nil, nil, err
		}
		delete(fields, lfsUploadReceiptJSONKey)
	}
	var candidateExpiresAt *time.Time
	if expiresJSON, ok := fields[lfsPendingExpiresAtJSONKey]; ok {
		var expiry time.Time
		if err := json.Unmarshal(expiresJSON, &expiry); err != nil {
			return nil, nil, nil, err
		}
		candidateExpiresAt = &expiry
		delete(fields, lfsPendingExpiresAtJSONKey)
	}
	return fields, receipt, candidateExpiresAt, nil
}

func (db *Store) requirePendingRegistrationTx(ctx context.Context, tx *sql.Tx, expected objects.PendingRegistration) error {
	var raw string
	var createdAt, expiresAt time.Time
	if err := db.txQueryRowContext(ctx, tx, `
		SELECT candidate_json, created_time, expires_time
		FROM lfs_pending_metadata WHERE oid = ?
	`, expected.OID).Scan(&raw, &createdAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: staged LFS metadata changed before registration", errorapi.ErrConflict)
		}
		return fmt.Errorf("load staged LFS metadata for registration: %w", err)
	}
	fields, receipt, candidateExpiresAt, err := decodePendingMetadataJSON(raw)
	if err != nil {
		return fmt.Errorf("decode staged LFS metadata for registration: %w", err)
	}
	if candidateExpiresAt == nil {
		candidateExpiresAt = &expiresAt
	}
	currentCandidate, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("encode staged LFS candidate for registration: %w", err)
	}
	var normalized lfsapi.DrsObjectCandidate
	if err := json.Unmarshal(currentCandidate, &normalized); err != nil {
		return fmt.Errorf("decode staged LFS candidate for registration: %w", err)
	}
	currentCandidate, err = json.Marshal(normalized)
	if err != nil {
		return fmt.Errorf("normalize staged LFS candidate for registration: %w", err)
	}
	currentReceipt, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("encode LFS receipt for registration: %w", err)
	}
	if receipt == nil || !createdAt.Equal(expected.CreatedAt) || !candidateExpiresAt.Equal(expected.ExpiresAt) || !candidateExpiresAt.After(time.Now().UTC()) || !bytes.Equal(currentCandidate, expected.CandidateJSON) || !bytes.Equal(currentReceipt, expected.ReceiptJSON) {
		return fmt.Errorf("%w: staged LFS metadata changed before registration", errorapi.ErrConflict)
	}
	return nil
}

// ConsumePendingMetadata removes the expected pending entry if it is still
// the entry that was read by the caller. A missing or replaced entry is an
// expected no-op: another stage or verification operation owns that metadata.
func (db *Store) ConsumePendingMetadata(ctx context.Context, expected transferlfs.PendingMetadata) (bool, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var (
		raw       string
		createdAt time.Time
		expiresAt time.Time
	)
	if err := db.txQueryRowContext(ctx, tx, `
		SELECT candidate_json, created_time, expires_time
		FROM lfs_pending_metadata
		WHERE oid = ?
	`, expected.OID).Scan(&raw, &createdAt, &expiresAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to load current pending metadata for oid %s: %w", expected.OID, err)
	}

	currentFields, receipt, candidateExpiresAt, err := decodePendingMetadataJSON(raw)
	if err != nil {
		return false, fmt.Errorf("failed to parse current pending metadata candidate for oid %s: %w", expected.OID, err)
	}
	if len(currentFields) == 0 {
		return false, nil
	}
	if candidateExpiresAt == nil {
		candidateExpiresAt = &expiresAt
	}
	currentJSON, err := json.Marshal(currentFields)
	if err != nil {
		return false, fmt.Errorf("failed to marshal current pending metadata candidate for oid %s: %w", expected.OID, err)
	}
	var current lfsapi.DrsObjectCandidate
	if err := json.Unmarshal(currentJSON, &current); err != nil {
		return false, fmt.Errorf("failed to decode current pending metadata candidate for oid %s: %w", expected.OID, err)
	}
	currentCanonical, err := json.Marshal(current)
	if err != nil {
		return false, fmt.Errorf("failed to marshal current pending metadata candidate for oid %s: %w", expected.OID, err)
	}
	expectedCanonical, err := json.Marshal(expected.Candidate)
	if err != nil {
		return false, fmt.Errorf("failed to marshal expected pending metadata candidate for oid %s: %w", expected.OID, err)
	}
	if !createdAt.Equal(expected.CreatedAt) || !candidateExpiresAt.Equal(expected.ExpiresAt) || !bytes.Equal(currentCanonical, expectedCanonical) || !sameUploadReceipt(receipt, expected.UploadReceipt) {
		return false, nil
	}

	candidateCondition := "candidate_json = ?"
	if strings.HasPrefix(db.dialect.Rebind("?"), "$") {
		candidateCondition = "candidate_json = CAST(? AS JSONB)"
	}
	query := fmt.Sprintf(`
		DELETE FROM lfs_pending_metadata
		WHERE oid = ? AND %s AND created_time = ? AND expires_time = ?
	`, candidateCondition)
	args := []any{expected.OID, raw, createdAt, expiresAt}
	if receipt != nil {
		receiptJSON, err := json.Marshal(receipt)
		if err != nil {
			return false, fmt.Errorf("failed to preserve LFS upload receipt for oid %s: %w", expected.OID, err)
		}
		remainingJSON, err := json.Marshal(map[string]json.RawMessage{lfsUploadReceiptJSONKey: receiptJSON})
		if err != nil {
			return false, fmt.Errorf("failed to preserve LFS upload receipt for oid %s: %w", expected.OID, err)
		}
		query = fmt.Sprintf(`
			UPDATE lfs_pending_metadata
			SET candidate_json = ?
			WHERE oid = ? AND %s AND created_time = ? AND expires_time = ?
		`, candidateCondition)
		if strings.HasPrefix(db.dialect.Rebind("?"), "$") {
			query = fmt.Sprintf(`
				UPDATE lfs_pending_metadata
				SET candidate_json = CAST(? AS JSONB)
				WHERE oid = ? AND %s AND created_time = ? AND expires_time = ?
			`, candidateCondition)
		}
		args = []any{string(remainingJSON), expected.OID, raw, createdAt, expiresAt}
	}
	result, err := db.txExecContext(ctx, tx, query, args...)
	if err != nil {
		return false, fmt.Errorf("failed to consume pending metadata for oid %s: %w", expected.OID, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return false, fmt.Errorf("failed to inspect pending metadata consumption for oid %s: %w", expected.OID, err)
	} else if affected == 0 {
		return false, nil
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("failed to commit pending metadata consumption for oid %s: %w", expected.OID, err)
	}
	return true, nil
}

func sameUploadReceipt(left, right *transferlfs.UploadReceipt) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.OID == right.OID && left.Size == right.Size && left.SHA256 == right.SHA256 && left.StorageURL == right.StorageURL && left.CompletedAt.Equal(right.CompletedAt) && left.ExpiresAt.Equal(right.ExpiresAt)
}
