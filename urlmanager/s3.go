package urlmanager

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3MultipartBackend struct {
	m    *Manager
	item *cacheItem
}

func (b *s3MultipartBackend) Init(ctx context.Context, bucketName string, key string) (string, error) {
	if b.item.S3Client == nil {
		return "", fmt.Errorf("missing s3 client for bucket %s", bucketName)
	}
	out, err := b.item.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload: %w", err)
	}
	return aws.ToString(out.UploadId), nil
}

func (b *s3MultipartBackend) SignPart(ctx context.Context, bucketName string, key string, uploadID string, partNumber int32) (string, error) {
	if b.item.S3Presigner == nil {
		return "", fmt.Errorf("missing s3 presign client for bucket %s", bucketName)
	}
	expirySeconds := b.m.signing.DefaultExpirySeconds
	if expirySeconds <= 0 {
		expirySeconds = 900
	}
	req, err := b.item.S3Presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadID),
		PartNumber: aws.Int32(partNumber),
	}, func(o *s3.PresignOptions) {
		o.Expires = time.Duration(expirySeconds) * time.Second
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign multipart part: %w", err)
	}
	return req.URL, nil
}

func (b *s3MultipartBackend) Complete(ctx context.Context, bucketName string, key string, uploadID string, parts []MultipartPart) error {
	if b.item.S3Client == nil {
		return fmt.Errorf("missing s3 client for bucket %s", bucketName)
	}
	partList := normalizedMultipartParts(parts)
	completedParts := make([]types.CompletedPart, 0, len(partList))
	for _, p := range partList {
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		})
	}
	_, err := b.item.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}
	return nil
}
