package urlmanager

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// BlobUrlManager implements UrlManager for multiple providers using go-cloud.
// It uses standard go-cloud URL openers (s3://, gs://, azblob://, file://)
// to support multiple cloud providers without specific bare-bones client coupling.
type BlobUrlManager struct {
	cache sync.Map
}

func NewBlobUrlManager() *BlobUrlManager {
	return &BlobUrlManager{}
}

func (m *BlobUrlManager) getBucket(ctx context.Context, bucketURL string) (*blob.Bucket, error) {
	if val, ok := m.cache.Load(bucketURL); ok {
		return val.(*blob.Bucket), nil
	}

	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket %s: %w", bucketURL, err)
	}

	m.cache.Store(bucketURL, bucket)
	slog.Debug("cached blob bucket", "url", bucketURL)
	return bucket, nil
}

func (m *BlobUrlManager) parseURL(urlStr string) (bucketURL string, key string, err error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme == "" {
		return "", "", fmt.Errorf("missing scheme in url: %s", urlStr)
	}

	if u.Scheme == "file" {
		// handle file:// paths via standard go-cloud URL semantics
		bucketURL = "file:///"
		key = strings.TrimPrefix(u.Path, "/")
		if u.Host != "" {
			key = u.Host + "/" + key
		}
		return bucketURL, key, nil
	}

	bucketURL = fmt.Sprintf("%s://%s", u.Scheme, u.Host)
	key = strings.TrimPrefix(u.Path, "/")
	return bucketURL, key, nil
}

func (m *BlobUrlManager) SignURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	bucketURL, key, err := m.parseURL(urlStr)
	if err != nil {
		return "", err
	}

	bucket, err := m.getBucket(ctx, bucketURL)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	return bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodGet,
	})
}

func (m *BlobUrlManager) SignUploadURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	bucketURL, key, err := m.parseURL(urlStr)
	if err != nil {
		return "", err
	}

	bucket, err := m.getBucket(ctx, bucketURL)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	return bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodPut,
	})
}

// Multipart methods proxy to AWS SDK directly for S3 buckets.
func (m *BlobUrlManager) InitMultipartUpload(ctx context.Context, bucketStr string, key string) (string, error) {
	bucketURL := fmt.Sprintf("s3://%s", bucketStr)
	bucket, err := m.getBucket(ctx, bucketURL)
	if err != nil {
		return "", err
	}

	var s3Client *s3.Client
	if !bucket.As(&s3Client) {
		return "", fmt.Errorf("multipart presigned uploads only supported for explicit AWS S3 buckets (unwrapping failed)")
	}

	out, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketStr),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload: %w", err)
	}

	return *out.UploadId, nil
}

func (m *BlobUrlManager) SignMultipartPart(ctx context.Context, bucketStr string, key string, uploadId string, partNumber int32) (string, error) {
	bucketURL := fmt.Sprintf("s3://%s", bucketStr)
	bucket, err := m.getBucket(ctx, bucketURL)
	if err != nil {
		return "", err
	}

	var s3Client *s3.Client
	if !bucket.As(&s3Client) {
		return "", fmt.Errorf("multipart presigned uploads only supported for explicit AWS S3 buckets (unwrapping failed)")
	}

	presignClient := s3.NewPresignClient(s3Client)
	req, err := presignClient.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketStr),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(partNumber),
	}, func(o *s3.PresignOptions) {
		o.Expires = 15 * time.Minute
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign multipart part: %w", err)
	}

	return req.URL, nil
}

func (m *BlobUrlManager) CompleteMultipartUpload(ctx context.Context, bucketStr string, key string, uploadId string, parts []MultipartPart) error {
	bucketURL := fmt.Sprintf("s3://%s", bucketStr)
	bucket, err := m.getBucket(ctx, bucketURL)
	if err != nil {
		return err
	}

	var s3Client *s3.Client
	if !bucket.As(&s3Client) {
		return fmt.Errorf("multipart presigned uploads only supported for explicit AWS S3 buckets (unwrapping failed)")
	}

	completedParts := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		completedParts[i] = types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		}
	}

	_, err = s3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketStr),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return fmt.Errorf("failed to complete multipart upload: %w", err)
	}

	return nil
}
