package urlmanager

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/calypr/drs-server/db/core"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// s3CacheItem holds the blob.Bucket and the raw s3.Client
type s3CacheItem struct {
	Bucket   *blob.Bucket
	S3Client *s3.Client
}

// S3UrlManager implements UrlManager for AWS S3 using go-cloud.
type S3UrlManager struct {
	database core.DatabaseInterface
	// cache stores *s3CacheItem keyed by bucket name
	cache sync.Map
}

// NewS3UrlManager creating a new S3UrlManager.
// It requires a database connection to fetch credentials.
func NewS3UrlManager(database core.DatabaseInterface) *S3UrlManager {
	return &S3UrlManager{
		database: database,
	}
}

// getBucket retrieves a s3CacheItem for the given bucket name.
func (m *S3UrlManager) getBucket(ctx context.Context, bucketName string) (*s3CacheItem, error) {
	// Check cache
	if val, ok := m.cache.Load(bucketName); ok {
		return val.(*s3CacheItem), nil
	}

	// Fetch credentials from DB
	cred, err := m.database.GetS3Credential(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials for bucket %s: %w", bucketName, err)
	}

	// Create S3 Client
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cred.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cred.AccessKey, cred.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	// If endpoint is specified (e.g. for MinIO or testing), use it
	if cred.Endpoint != "" {
		endpoint := cred.Endpoint
		// Ensure scheme is present
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			if strings.Contains(endpoint, "localhost") || strings.Contains(endpoint, "127.0.0.1") {
				endpoint = "http://" + endpoint
			} else {
				endpoint = "https://" + endpoint
			}
		}
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	// Create S3 Client with options
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if cred.Endpoint != "" {
			o.UsePathStyle = true
		}
	})

	// Open bucket using s3blob
	bucket, err := s3blob.OpenBucket(ctx, client, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open s3 bucket: %w", err)
	}

	// Cache it
	item := &s3CacheItem{
		Bucket:   bucket,
		S3Client: client,
	}
	m.cache.Store(bucketName, item)

	return item, nil
}

// SignURL signs a URL for the given resource (Download).
func (m *S3UrlManager) SignURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme != "s3" {
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	bucketName := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	item, err := m.getBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	presignClient := s3.NewPresignClient(item.S3Client)
	req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}, func(o *s3.PresignOptions) {
		o.Expires = expiry
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign url: %w", err)
	}

	return req.URL, nil
}

// SignUploadURL signs a URL for uploading a resource.
func (m *S3UrlManager) SignUploadURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme != "s3" {
		return "", fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	bucketName := u.Host
	key := strings.TrimPrefix(u.Path, "/")

	item, err := m.getBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	presignClient := s3.NewPresignClient(item.S3Client)
	req, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	}, func(o *s3.PresignOptions) {
		o.Expires = expiry
	})

	if err != nil {
		return "", fmt.Errorf("failed to sign upload url: %w", err)
	}

	return req.URL, nil
}

// Multipart Support

func (m *S3UrlManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	item, err := m.getBucket(ctx, bucket)
	if err != nil {
		return "", err
	}

	out, err := item.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload: %w", err)
	}

	return *out.UploadId, nil
}

func (m *S3UrlManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	item, err := m.getBucket(ctx, bucket)
	if err != nil {
		return "", err
	}

	presignClient := s3.NewPresignClient(item.S3Client)
	req, err := presignClient.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
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

func (m *S3UrlManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []MultipartPart) error {
	item, err := m.getBucket(ctx, bucket)
	if err != nil {
		return err
	}

	completedParts := make([]types.CompletedPart, len(parts))
	for i, p := range parts {
		completedParts[i] = types.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int32(p.PartNumber),
		}
	}

	_, err = item.S3Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
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
