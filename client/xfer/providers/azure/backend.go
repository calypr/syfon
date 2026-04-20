package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/calypr/syfon/client/transfer"
)

type backend struct {
	logger    transfer.TransferLogger
	client    *azblob.Client
	container string
}

// NewBackend returns a native Azure implementation of transfer.Backend.
func NewBackend(logger transfer.TransferLogger, client *azblob.Client, container string) transfer.Backend {
	return &backend{
		logger:    logger,
		client:    client,
		container: container,
	}
}

func (b *backend) Name() string                    { return "NativeAzure" }
func (b *backend) Logger() transfer.TransferLogger { return b.logger }

func (b *backend) Validate(ctx context.Context, containerName string) error {
	if containerName == "" {
		containerName = b.container
	}
	_, err := b.client.ServiceClient().NewContainerClient(containerName).GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("azure container validation failed for %s: %w", containerName, err)
	}
	return nil
}

func (b *backend) Stat(ctx context.Context, key string) (*transfer.ObjectMetadata, error) {
	out, err := b.client.ServiceClient().NewContainerClient(b.container).NewBlobClient(key).GetProperties(ctx, nil)
	if err != nil {
		return nil, err
	}

	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	return &transfer.ObjectMetadata{
		Size:         size,
		MD5:          "", // Azure uses Content-MD5 header which is often base64
		AcceptRanges: true,
		Provider:     b.Name(),
	}, nil
}

func (b *backend) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := b.client.DownloadStream(ctx, b.container, key, nil)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *backend) GetRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	count := int64(blob.CountToEnd)
	if length > 0 {
		count = length
	}
	resp, err := b.client.DownloadStream(ctx, b.container, key, &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: offset,
			Count:  count,
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (b *backend) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("single-stream GetWriter not implemented for NativeAzure (use Multipart or Upload)")
}

func (b *backend) Upload(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := b.client.UploadStream(ctx, b.container, key, body, nil)
	return err
}

func (b *backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteBlob(ctx, b.container, key, nil)
	return err
}

func (b *backend) MultipartInit(ctx context.Context, key string) (string, error) {
	// Azure doesn't require explicit init.
	return "azure-session", nil
}

func (b *backend) MultipartPart(ctx context.Context, key string, uploadID string, partNum int, body io.Reader) (string, error) {
	// Block ID must be base64 encoded and of fixed length.
	blockID := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%07d", partNum)))

	bbClient := b.client.ServiceClient().NewContainerClient(b.container).NewBlockBlobClient(key)
	payload, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	_, err = bbClient.StageBlock(ctx, blockID, readSeekCloser{Reader: bytes.NewReader(payload)}, nil)
	if err != nil {
		return "", err
	}
	return blockID, nil
}

func (b *backend) MultipartComplete(ctx context.Context, key string, uploadID string, parts []transfer.MultipartPart) error {
	blockIDs := make([]string, 0, len(parts))
	for _, p := range parts {
		blockIDs = append(blockIDs, p.ETag) // Block ID was returned as ETag
	}

	bbClient := b.client.ServiceClient().NewContainerClient(b.container).NewBlockBlobClient(key)
	_, err := bbClient.CommitBlockList(ctx, blockIDs, nil)
	return err
}

type readSeekCloser struct {
	*bytes.Reader
}

func (r readSeekCloser) Close() error { return nil }
