package gcs

import (
	"context"
	"fmt"
	"io"
	"path"

	"cloud.google.com/go/storage"
	"github.com/calypr/syfon/client/transfer"
	"github.com/google/uuid"
)

type backend struct {
	logger transfer.TransferLogger
	client *storage.Client
	bucket string
}

// NewBackend returns a native GCS implementation of transfer.Backend.
func NewBackend(logger transfer.TransferLogger, client *storage.Client, bucket string) transfer.Backend {
	return &backend{
		logger: logger,
		client: client,
		bucket: bucket,
	}
}

func (b *backend) Name() string                    { return "NativeGCS" }
func (b *backend) Logger() transfer.TransferLogger { return b.logger }

func (b *backend) Validate(ctx context.Context, bucket string) error {
	if bucket == "" {
		bucket = b.bucket
	}
	_, err := b.client.Bucket(bucket).Attrs(ctx)
	if err != nil {
		return fmt.Errorf("gcs bucket validation failed for %s: %w", bucket, err)
	}
	return nil
}

func (b *backend) Stat(ctx context.Context, key string) (*transfer.ObjectMetadata, error) {
	attrs, err := b.client.Bucket(b.bucket).Object(key).Attrs(ctx)
	if err != nil {
		return nil, err
	}

	return &transfer.ObjectMetadata{
		Size:         attrs.Size,
		MD5:          fmt.Sprintf("%x", attrs.MD5),
		AcceptRanges: true,
		Provider:     b.Name(),
	}, nil
}

func (b *backend) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return b.client.Bucket(b.bucket).Object(key).NewReader(ctx)
}

func (b *backend) GetRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return b.client.Bucket(b.bucket).Object(key).NewRangeReader(ctx, offset, length)
}

func (b *backend) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return b.client.Bucket(b.bucket).Object(key).NewWriter(ctx), nil
}

func (b *backend) Upload(ctx context.Context, key string, body io.Reader, size int64) error {
	wc := b.client.Bucket(b.bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(wc, body); err != nil {
		wc.Close()
		return err
	}
	return wc.Close()
}

func (b *backend) Delete(ctx context.Context, key string) error {
	return b.client.Bucket(b.bucket).Object(key).Delete(ctx)
}

func (b *backend) MultipartInit(ctx context.Context, key string) (string, error) {
	// GCS doesn't have native multipart init in the same way.
	// We'll use a UUID as an upload session ID to group compose parts.
	return uuid.NewString(), nil
}

func (b *backend) MultipartPart(ctx context.Context, key string, uploadID string, partNum int, body io.Reader) (string, error) {
	// Upload part to a temporary location.
	tempKey := path.Join(".syfon-multipart", uploadID, fmt.Sprintf("part-%d", partNum))
	wc := b.client.Bucket(b.bucket).Object(tempKey).NewWriter(ctx)
	if _, err := io.Copy(wc, body); err != nil {
		wc.Close()
		return "", err
	}
	if err := wc.Close(); err != nil {
		return "", err
	}
	// Return the tempKey as the ETag so Complete knows what to compose.
	return tempKey, nil
}

func (b *backend) MultipartComplete(ctx context.Context, key string, uploadID string, parts []transfer.MultipartPart) error {
	if len(parts) == 0 {
		return fmt.Errorf("no parts provided for multipart complete")
	}

	// Process parts in batches of 32 (GCS limit for ComposerFrom)
	var currentParts []string
	for _, p := range parts {
		currentParts = append(currentParts, p.ETag) // We stored tempKey in ETag
	}

	tempKeys, err := b.composeAll(ctx, key, uploadID, currentParts)
	if err != nil {
		return err
	}

	// Cleanup temp files
	for _, k := range append(currentParts, tempKeys...) {
		if err := b.client.Bucket(b.bucket).Object(k).Delete(ctx); err != nil {
			return fmt.Errorf("delete multipart temp object %s: %w", k, err)
		}
	}

	return nil
}

func (b *backend) composeAll(ctx context.Context, destKey, uploadID string, partKeys []string) ([]string, error) {
	current := append([]string(nil), partKeys...)
	tempKeys := []string{}
	round := 0

	for len(current) > 32 {
		next := []string{}
		for i := 0; i < len(current); i += 32 {
			end := i + 32
			if end > len(current) {
				end = len(current)
			}
			tmp := path.Join(".syfon-multipart", uploadID, "compose", fmt.Sprintf("%d-%d", round, i/32))
			if err := b.composeBatch(ctx, tmp, current[i:end]); err != nil {
				return tempKeys, err
			}
			tempKeys = append(tempKeys, tmp)
			next = append(next, tmp)
		}
		current = next
		round++
	}

	if err := b.composeBatch(ctx, destKey, current); err != nil {
		return tempKeys, err
	}
	return tempKeys, nil
}

func (b *backend) composeBatch(ctx context.Context, dst string, src []string) error {
	dstObj := b.client.Bucket(b.bucket).Object(dst)
	srcObjs := make([]*storage.ObjectHandle, 0, len(src))
	for _, k := range src {
		srcObjs = append(srcObjs, b.client.Bucket(b.bucket).Object(k))
	}
	if _, err := dstObj.ComposerFrom(srcObjs...).Run(ctx); err != nil {
		return fmt.Errorf("failed gcs compose for %s: %w", dst, err)
	}
	return nil
}
