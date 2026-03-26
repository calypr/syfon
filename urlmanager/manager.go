package urlmanager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/internal/provider"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
)

// cacheItem holds the blob.Bucket and any provider-specific clients (e.g. s3.Client).
type cacheItem struct {
	Bucket   *blob.Bucket
	S3Client *s3.Client
}

// Manager is the unified implementation of UrlManager.
// It handles multicloud signing and multipart uploads by resolving 
// provider metadata from the database.
type Manager struct {
	database        core.DatabaseInterface
	signing         config.SigningConfig
	defaultProvider string
	cache           sync.Map // keyed by bucket name
}

func NewManager(database core.DatabaseInterface, signing config.SigningConfig) *Manager {
	return &Manager{
		database:        database,
		signing:         signing,
		defaultProvider: provider.S3,
	}
}

func (m *Manager) SignURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	bucketName, key, p, err := m.resolve(ctx, accessId, urlStr)
	if err != nil {
		return "", err
	}

	item, err := m.getBucket(ctx, bucketName, p)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	if p == provider.S3 && item.S3Client != nil {
		presignClient := s3.NewPresignClient(item.S3Client)
		req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}, func(o *s3.PresignOptions) {
			o.Expires = expiry
		})
		if err != nil {
			return "", fmt.Errorf("failed to sign s3 url: %w", err)
		}
		return req.URL, nil
	}

	signed, err := item.Bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodGet,
	})
	if err != nil {
		// If signing is not supported by the driver (e.g. fileblob), fallback to the original URL.
		if strings.Contains(strings.ToLower(err.Error()), "unimplemented") || strings.Contains(strings.ToLower(err.Error()), "not supported") {
			return urlStr, nil
		}
		return "", err
	}
	return signed, nil
}

func (m *Manager) SignUploadURL(ctx context.Context, accessId string, urlStr string, opts SignOptions) (string, error) {
	bucketName, key, p, err := m.resolve(ctx, accessId, urlStr)
	if err != nil {
		return "", err
	}

	item, err := m.getBucket(ctx, bucketName, p)
	if err != nil {
		return "", err
	}

	expiry := 15 * time.Minute
	if opts.ExpiresIn > 0 {
		expiry = time.Duration(opts.ExpiresIn) * time.Second
	}

	if p == provider.S3 && item.S3Client != nil {
		presignClient := s3.NewPresignClient(item.S3Client)
		req, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}, func(o *s3.PresignOptions) {
			o.Expires = expiry
		})
		if err != nil {
			return "", fmt.Errorf("failed to sign s3 upload url: %w", err)
		}
		return req.URL, nil
	}

	signed, err := item.Bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodPut,
	})
	if err != nil {
		// If signing is not supported by the driver (e.g. fileblob), fallback to the original URL.
		if strings.Contains(strings.ToLower(err.Error()), "unimplemented") || strings.Contains(strings.ToLower(err.Error()), "not supported") {
			return urlStr, nil
		}
		return "", err
	}
	return signed, nil
}

func (m *Manager) InitMultipartUpload(ctx context.Context, bucketName string, key string) (string, error) {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}
	if p != provider.S3 {
		return "", fmt.Errorf("multipart uploads only supported for explicit AWS S3 buckets (provider=%s)", p)
	}

	item, err := m.getBucket(ctx, bucketName, p)
	if err != nil {
		return "", err
	}

	out, err := item.S3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("failed to init multipart upload: %w", err)
	}

	return *out.UploadId, nil
}

func (m *Manager) SignMultipartPart(ctx context.Context, bucketName string, key string, uploadId string, partNumber int32) (string, error) {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}
	if p != provider.S3 {
		return "", fmt.Errorf("multipart uploads only supported for explicit AWS S3 buckets")
	}

	item, err := m.getBucket(ctx, bucketName, p)
	if err != nil {
		return "", err
	}

	presignClient := s3.NewPresignClient(item.S3Client)
	req, err := presignClient.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(bucketName),
		Key:        aws.String(key),
		UploadId:   aws.String(uploadId),
		PartNumber: aws.Int32(partNumber),
	}, func(o *s3.PresignOptions) {
		o.Expires = time.Duration(m.signing.DefaultExpirySeconds) * time.Second
	})
	if err != nil {
		return "", fmt.Errorf("failed to sign multipart part: %w", err)
	}

	return req.URL, nil
}

func (m *Manager) CompleteMultipartUpload(ctx context.Context, bucketName string, key string, uploadId string, parts []MultipartPart) error {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	if p != provider.S3 {
		return fmt.Errorf("multipart uploads only supported for explicit AWS S3 buckets")
	}

	item, err := m.getBucket(ctx, bucketName, p)
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
		Bucket:   aws.String(bucketName),
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

func (m *Manager) resolve(ctx context.Context, accessId string, urlStr string) (bucket string, key string, p string, err error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "", "", "", fmt.Errorf("failed to parse url: %w", err)
	}

	if u.Scheme == "file" {
		bucket = config.FilePrefix
		key = strings.TrimPrefix(u.Path, "/")
		if u.Host != "" {
			key = u.Host + "/" + key
		}
		return bucket, key, provider.File, nil
	}

	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")

	// Resolve provider
	candidates := []string{bucket, accessId}
	for _, c := range candidates {
		candidate := strings.TrimSpace(c)
		if candidate == "" {
			continue
		}
		if res, err := m.resolveProviderForBucket(ctx, candidate); err == nil {
			return bucket, key, res, nil
		}
	}

	// Fallback to URL scheme
	schemeProvider := provider.FromScheme(u.Scheme)
	p = provider.Normalize(schemeProvider, m.defaultProvider)
	return bucket, key, p, nil
}

func (m *Manager) resolveProviderForBucket(ctx context.Context, bucket string) (string, error) {
	cred, err := m.database.GetS3Credential(ctx, strings.TrimSpace(bucket))
	if err != nil {
		return "", err
	}
	return provider.Normalize(cred.Provider, m.defaultProvider), nil
}

func (m *Manager) getBucket(ctx context.Context, bucketName string, p string) (*cacheItem, error) {
	if val, ok := m.cache.Load(bucketName); ok {
		return val.(*cacheItem), nil
	}

	var item *cacheItem
	var err error

	switch p {
	case provider.S3:
		item, err = m.openS3Bucket(ctx, bucketName)
	case provider.File:
		bucket, bErr := blob.OpenBucket(ctx, config.FilePrefix)
		if bErr == nil {
			item = &cacheItem{Bucket: bucket}
		} else {
			err = bErr
		}
	case provider.GCS:
		bucket, bErr := blob.OpenBucket(ctx, config.GCSPrefix+bucketName)
		if bErr == nil {
			item = &cacheItem{Bucket: bucket}
		} else {
			err = bErr
		}
	case provider.Azure:
		bucket, bErr := blob.OpenBucket(ctx, config.AzurePrefix+bucketName)
		if bErr == nil {
			item = &cacheItem{Bucket: bucket}
		} else {
			err = bErr
		}
	default:
		return nil, fmt.Errorf("unsupported provider: %s", p)
	}

	if err != nil {
		return nil, err
	}

	m.cache.Store(bucketName, item)
	return item, nil
}

func (m *Manager) openS3Bucket(ctx context.Context, bucketName string) (*cacheItem, error) {
	cred, err := m.database.GetS3Credential(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials for bucket %s: %w", bucketName, err)
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cred.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cred.AccessKey, cred.SecretKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	if cred.Endpoint != "" {
		endpoint := cred.Endpoint
		if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
			if strings.Contains(endpoint, "localhost") || strings.Contains(endpoint, "127.0.0.1") {
				endpoint = "http://" + endpoint
			} else {
				endpoint = "https://" + endpoint
			}
		}
		cfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if cred.Endpoint != "" {
			o.UsePathStyle = true
		}
	})

	bucket, err := s3blob.OpenBucket(ctx, client, bucketName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open s3 bucket: %w", err)
	}

	return &cacheItem{
		Bucket:   bucket,
		S3Client: client,
	}, nil
}
