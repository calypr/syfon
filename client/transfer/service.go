package transfer

import (
	"context"
	"fmt"
	"io"

	"github.com/calypr/syfon/client/pkg/hash"
	"github.com/calypr/syfon/client/pkg/logs"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// Provider is the minimal interface a capability needs to resolve its bucket/key.
type Provider interface {
	GetStorageLocation(ctx context.Context, guid string) (bucket, key string, err error)
}

// client is the Go Cloud adapter implementation of Backend.
type client struct {
	logger   *logs.Gen3Logger
	provider Provider
}

// New returns a unified transfer adapter sitting on top of the go-cloud blob package.
func New(logger *logs.Gen3Logger, provider Provider) Backend {
	return &client{
		logger:   logger,
		provider: provider,
	}
}

func (c *client) Name() string             { return "GoCloudBackend" }
func (c *client) Logger() *logs.Gen3Logger { return c.logger }

func (c *client) Stat(ctx context.Context, guid string) (*ObjectMetadata, error) {
	bucket, key, err := c.provider.GetStorageLocation(ctx, guid)
	if err != nil {
		return nil, err
	}
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	defer b.Close()

	attr, err := b.Attributes(ctx, key)
	if err != nil {
		return nil, err
	}

	return &ObjectMetadata{
		Size:         attr.Size,
		MD5:          fmt.Sprintf("%x", attr.MD5),
		AcceptRanges: true,
		Provider:     c.Name(),
	}, nil
}

func (c *client) GetReader(ctx context.Context, guid string) (io.ReadCloser, error) {
	bucket, key, err := c.provider.GetStorageLocation(ctx, guid)
	if err != nil {
		return nil, err
	}
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return b.NewReader(ctx, key, nil)
}

func (c *client) GetRangeReader(ctx context.Context, guid string, offset, length int64) (io.ReadCloser, error) {
	bucket, key, err := c.provider.GetStorageLocation(ctx, guid)
	if err != nil {
		return nil, err
	}
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return b.NewRangeReader(ctx, key, offset, length, nil)
}

func (c *client) GetWriter(ctx context.Context, guid string) (io.WriteCloser, error) {
	bucket, key, err := c.provider.GetStorageLocation(ctx, guid)
	if err != nil {
		return nil, err
	}
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, key, nil)
}

func (c *client) Delete(ctx context.Context, guid string) error {
	bucket, key, err := c.provider.GetStorageLocation(ctx, guid)
	if err != nil {
		return err
	}
	b, err := blob.OpenBucket(ctx, bucket)
	if err != nil {
		return err
	}
	defer b.Close()
	return b.Delete(ctx, key)
}

func (c *client) MultipartInit(ctx context.Context, guid string) (string, error) {
	return "", fmt.Errorf("manual multipart init not implemented for Go Cloud adapter (use GetWriter)")
}

func (c *client) MultipartPart(ctx context.Context, guid string, uploadID string, partNum int, body io.Reader) (string, error) {
	return "", fmt.Errorf("manual multipart part not implemented for Go Cloud adapter (use GetWriter)")
}

func (c *client) MultipartComplete(ctx context.Context, guid string, uploadID string, parts []hash.HashInfo) error {
	return fmt.Errorf("manual multipart complete not implemented for Go Cloud adapter (use GetWriter)")
}
