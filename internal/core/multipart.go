package core

import (
	"context"
	"fmt"

	"github.com/calypr/syfon/internal/storage"
	"github.com/calypr/syfon/internal/transfers"
)

func (m *ObjectManager) SavePendingLFSMeta(ctx context.Context, entries []transfers.PendingMetadata) error {
	return m.pendingStore.SavePendingLFSMeta(ctx, entries)
}

func (m *ObjectManager) GetPendingLFSMeta(ctx context.Context, oid string) (*transfers.PendingMetadata, error) {
	return m.pendingStore.GetPendingLFSMeta(ctx, oid)
}

func (m *ObjectManager) PopPendingLFSMeta(ctx context.Context, oid string) (*transfers.PendingMetadata, error) {
	return m.pendingStore.PopPendingLFSMeta(ctx, oid)
}

func (m *ObjectManager) InitMultipartUpload(ctx context.Context, bucket, key string) (string, error) {
	if m.storageMultipart == nil {
		return "", fmt.Errorf("storage multipart is not configured")
	}
	uploadID, err := m.storageMultipart.BeginMultipart(ctx, storage.ObjectTarget{Bucket: bucket, Key: key})
	return string(uploadID), err
}

func (m *ObjectManager) SignMultipartPart(ctx context.Context, bucket, key, uploadID string, partNum int32) (string, error) {
	if m.storageMultipart == nil {
		return "", fmt.Errorf("storage multipart is not configured")
	}
	access, err := m.storageMultipart.AccessMultipartPart(ctx, storage.MultipartPartRequest{
		Target:     storage.ObjectTarget{Bucket: bucket, Key: key},
		UploadID:   storage.UploadID(uploadID),
		PartNumber: partNum,
	})
	if err != nil {
		return "", err
	}
	return access.Location, nil
}

func (m *ObjectManager) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.CompletedPart) error {
	if m.storageMultipart == nil {
		return fmt.Errorf("storage multipart is not configured")
	}
	return m.storageMultipart.CompleteMultipart(ctx, storage.CompleteMultipartRequest{
		Target:   storage.ObjectTarget{Bucket: bucket, Key: key},
		UploadID: storage.UploadID(uploadID),
		Parts:    parts,
	})
}
