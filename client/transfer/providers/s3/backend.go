package s3

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/calypr/syfon/client/transfer"
)

type backend struct {
	logger transfer.TransferLogger
	client *s3.Client
	bucket string
}

// NewBackend returns a native S3 implementation of transfer.Backend.
func NewBackend(logger transfer.TransferLogger, client *s3.Client, bucket string) transfer.Backend {
	return &backend{
		logger: logger,
		client: client,
		bucket: bucket,
	}
}

func (b *backend) Name() string                    { return "NativeS3" }
func (b *backend) Logger() transfer.TransferLogger { return b.logger }

func (b *backend) Validate(ctx context.Context, bucket string) error {
	if bucket == "" {
		bucket = b.bucket
	}
	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("s3 bucket validation failed for %s: %w", bucket, err)
	}
	return nil
}

func (b *backend) Stat(ctx context.Context, key string) (*transfer.ObjectMetadata, error) {
	out, err := b.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	return &transfer.ObjectMetadata{
		Size:         size,
		MD5:          strings.Trim(aws.ToString(out.ETag), `"`),
		AcceptRanges: aws.ToString(out.AcceptRanges) == "bytes",
		Provider:     b.Name(),
	}, nil
}

func (b *backend) GetReader(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (b *backend) GetRangeReader(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rangeHeader := fmt.Sprintf("bytes=%d-", offset)
	if length > 0 {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	}

	out, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeHeader),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (b *backend) GetWriter(ctx context.Context, key string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("single-stream GetWriter not implemented for NativeS3 (use Multipart or Upload)")
}

func (b *backend) Upload(ctx context.Context, key string, body io.Reader, size int64) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(b.bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(size),
	})
	return err
}

func (b *backend) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (b *backend) MultipartInit(ctx context.Context, key string) (string, error) {
	out, err := b.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.UploadId), nil
}

func (b *backend) MultipartPart(ctx context.Context, key string, uploadID string, partNum int, body io.Reader) (string, error) {
	out, err := b.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(b.bucket),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(int32(partNum)),
		Body:       body,
	})
	if err != nil {
		return "", err
	}
	return strings.Trim(aws.ToString(out.ETag), `"`), nil
}

func (b *backend) MultipartComplete(ctx context.Context, key string, uploadID string, parts []transfer.MultipartPart) error {
	completedParts := make([]types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		})
	}

	_, err := b.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(b.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	return err
}
