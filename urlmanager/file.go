package urlmanager

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/calypr/syfon/config"
	"github.com/google/uuid"
	"gocloud.dev/blob"
)

type fileMultipartBackend struct {
	m    *Manager
	item *cacheItem
}

func (b *fileMultipartBackend) Init(ctx context.Context, bucketName string, key string) (string, error) {
	return uuid.NewString(), nil
}

func (b *fileMultipartBackend) SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error) {
	partKey := multipartPartObjectKey(key, uploadID, partNumber)
	return signUploadKey(ctx, b.item.Bucket, partKey, b.m.signing)
}

func (b *fileMultipartBackend) Complete(ctx context.Context, bucketName string, key string, uploadID string, parts []MultipartPart) error {
	return completeMultipartByStitching(ctx, b.item.Bucket, key, uploadID, parts)
}

func signUploadKey(ctx context.Context, bucket *blob.Bucket, key string, signing config.SigningConfig) (string, error) {
	expirySeconds := signing.DefaultExpirySeconds
	if expirySeconds <= 0 {
		expirySeconds = 900
	}
	signed, err := bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: time.Duration(expirySeconds) * time.Second,
		Method: http.MethodPut,
	})
	if err != nil {
		if isSigningNotSupported(err) {
			return "", fmt.Errorf("provider does not support multipart presigned part uploads: %w", err)
		}
		return "", err
	}
	return signed, nil
}

func completeMultipartByStitching(ctx context.Context, bucket *blob.Bucket, key string, uploadID string, parts []MultipartPart) error {
	if len(parts) == 0 {
		return fmt.Errorf("multipart complete requires at least one part")
	}
	partList := normalizedMultipartParts(parts)
	for i, p := range partList {
		if p.PartNumber <= 0 {
			return fmt.Errorf("invalid multipart part number: %d", p.PartNumber)
		}
		if i > 0 && p.PartNumber == partList[i-1].PartNumber {
			return fmt.Errorf("duplicate multipart part number: %d", p.PartNumber)
		}
	}
	writer, err := bucket.NewWriter(ctx, strings.Trim(strings.TrimSpace(key), "/"), nil)
	if err != nil {
		return fmt.Errorf("failed to open destination writer: %w", err)
	}
	defer writer.Close()

	cleanupKeys := make([]string, 0, len(partList))
	for _, p := range partList {
		partKey := multipartPartObjectKey(key, uploadID, p.PartNumber)
		reader, err := bucket.NewReader(ctx, partKey, nil)
		if err != nil {
			return fmt.Errorf("failed to open multipart part %d: %w", p.PartNumber, err)
		}
		if _, err := io.Copy(writer, reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("failed to copy multipart part %d: %w", p.PartNumber, err)
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("failed to close multipart part %d reader: %w", p.PartNumber, err)
		}
		cleanupKeys = append(cleanupKeys, partKey)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to finalize multipart object: %w", err)
	}
	for _, partKey := range cleanupKeys {
		_ = bucket.Delete(ctx, partKey)
	}
	return nil
}
