package urlmanager

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/internal/provider"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/gcsblob"
	"gocloud.dev/blob/s3blob"
)

// cacheItem holds the blob.Bucket and any provider-specific clients (e.g. s3.Client).
type cacheItem struct {
	Bucket          *blob.Bucket
	S3Client        *s3.Client
	S3Presigner     *s3.PresignClient
	GCSClient       *storage.Client
	AzureSharedKey  *azblob.SharedKeyCredential
	AzureServiceURL string
	Provider        string
	BucketName      string
	SignerMissing   bool
}

// Manager is the unified implementation of UrlManager.
// It handles multicloud signing and multipart uploads by resolving
// provider metadata from the database.
type Manager struct {
	database        core.DatabaseInterface
	signing         config.SigningConfig
	defaultProvider string
	cache           sync.Map // keyed by bucket/provider and credential namespaces
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

	if p == provider.Azure && item.AzureSharedKey != nil && strings.TrimSpace(item.AzureServiceURL) != "" {
		signed, pErr := azureSignedURL(item.AzureServiceURL, bucketName, key, http.MethodGet, expiry, "", item.AzureSharedKey)
		if pErr == nil {
			return signed, nil
		}
	}

	signed, err := item.Bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodGet,
	})
	if err == nil {
		return signed, nil
	}

	// If driver signing is unsupported (or unavailable), fallback to provider-specific presigning.
	if isSigningNotSupported(err) {
		switch p {
		case provider.S3:
			if item.S3Presigner != nil {
				req, pErr := item.S3Presigner.PresignGetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				}, func(o *s3.PresignOptions) {
					o.Expires = expiry
				})
				if pErr == nil {
					return req.URL, nil
				}
			}
		case provider.GCS:
			if cred, cErr := m.credentialForBucket(ctx, bucketName); cErr == nil {
				signed, pErr := gcsSignedURL(bucketName, key, http.MethodGet, expiry, "", cred, m.signing)
				if pErr == nil {
					return signed, nil
				}
			}
			if item.AzureSharedKey != nil {
				signed, pErr := azureSignedURL(item.AzureServiceURL, bucketName, key, http.MethodGet, expiry, "", item.AzureSharedKey)
				if pErr == nil {
					return signed, nil
				}
			}
		}
		return urlStr, nil
	}
	return "", err
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

	if p == provider.Azure && item.AzureSharedKey != nil && strings.TrimSpace(item.AzureServiceURL) != "" {
		signed, pErr := azureSignedURL(item.AzureServiceURL, bucketName, key, http.MethodPut, expiry, "", item.AzureSharedKey)
		if pErr == nil {
			return signed, nil
		}
	}

	signed, err := item.Bucket.SignedURL(ctx, key, &blob.SignedURLOptions{
		Expiry: expiry,
		Method: http.MethodPut,
	})
	if err == nil {
		return signed, nil
	}

	// If driver signing is unsupported (or unavailable), fallback to provider-specific presigning.
	if isSigningNotSupported(err) {
		switch p {
		case provider.S3:
			if item.S3Presigner != nil {
				req, pErr := item.S3Presigner.PresignPutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucketName),
					Key:    aws.String(key),
				}, func(o *s3.PresignOptions) {
					o.Expires = expiry
				})
				if pErr == nil {
					return req.URL, nil
				}
			}
		case provider.GCS:
			if cred, cErr := m.credentialForBucket(ctx, bucketName); cErr == nil {
				signed, pErr := gcsSignedURL(bucketName, key, http.MethodPut, expiry, "", cred, m.signing)
				if pErr == nil {
					return signed, nil
				}
			}
			if item.AzureSharedKey != nil {
				signed, pErr := azureSignedURL(item.AzureServiceURL, bucketName, key, http.MethodPut, expiry, "", item.AzureSharedKey)
				if pErr == nil {
					return signed, nil
				}
			}
		}
		return urlStr, nil
	}
	return "", err
}

func (m *Manager) SignDownloadPart(ctx context.Context, accessId string, urlStr string, start int64, end int64, opts SignOptions) (string, error) {
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

	rangeStr := fmt.Sprintf("bytes=%d-%d", start, end)

	switch p {
	case provider.S3:
		if item.S3Presigner != nil {
			req, pErr := item.S3Presigner.PresignGetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(key),
				Range:  aws.String(rangeStr),
			}, func(o *s3.PresignOptions) {
				o.Expires = expiry
			})
			if pErr == nil {
				return req.URL, nil
			}
		}
	case provider.GCS:
		if cred, cErr := m.credentialForBucket(ctx, bucketName); cErr == nil {
			signed, pErr := gcsSignedURL(bucketName, key, http.MethodGet, expiry, rangeStr, cred, m.signing)
			if pErr == nil {
				return signed, nil
			}
		}
	case provider.Azure:
		if item.AzureSharedKey != nil {
			signed, pErr := azureSignedURL(item.AzureServiceURL, bucketName, key, http.MethodGet, expiry, rangeStr, item.AzureSharedKey)
			if pErr == nil {
				return signed, nil
			}
		}
	}

	return m.SignURL(ctx, accessId, urlStr, opts)
}

func (m *Manager) InitMultipartUpload(ctx context.Context, bucketName string, key string) (string, error) {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}
	backend, err := m.getMultipartBackend(ctx, bucketName, p)
	if err != nil {
		return "", err
	}
	return backend.Init(ctx, bucketName, key)
}

func (m *Manager) SignMultipartPart(ctx context.Context, bucketName string, key string, uploadId string, partNumber int32) (string, error) {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return "", err
	}
	backend, err := m.getMultipartBackend(ctx, bucketName, p)
	if err != nil {
		return "", err
	}
	return backend.SignPart(ctx, bucketName, key, uploadId, partNumber)
}

func (m *Manager) CompleteMultipartUpload(ctx context.Context, bucketName string, key string, uploadId string, parts []MultipartPart) error {
	p, err := m.resolveProviderForBucket(ctx, bucketName)
	if err != nil {
		return err
	}
	backend, err := m.getMultipartBackend(ctx, bucketName, p)
	if err != nil {
		return err
	}
	return backend.Complete(ctx, bucketName, key, uploadId, parts)
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
	cred, err := m.credentialForBucket(ctx, bucket)
	if err != nil {
		return "", err
	}
	return provider.Normalize(cred.Provider, m.defaultProvider), nil
}

func (m *Manager) credentialForBucket(ctx context.Context, bucket string) (*core.S3Credential, error) {
	key := strings.TrimSpace(bucket)
	if key == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	credKey := "cred|" + key
	if cached, ok := m.cache.Load(credKey); ok {
		if c, ok := cached.(core.S3Credential); ok {
			cp := c
			return &cp, nil
		}
	}

	cred, err := m.database.GetS3Credential(ctx, key)
	if err != nil {
		return nil, err
	}
	if cred == nil {
		return nil, fmt.Errorf("credential not found")
	}
	m.cache.Store(credKey, *cred)
	return cred, nil
}

func (m *Manager) getBucket(ctx context.Context, bucketName string, p string) (*cacheItem, error) {
	cacheKey := providerBucketCacheKey(p, bucketName)
	if val, ok := m.cache.Load(cacheKey); ok {
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
		item, err = m.openGCSBucket(ctx, bucketName)
	case provider.Azure:
		item, err = m.openAzureBucket(ctx, bucketName)
	default:
		return nil, fmt.Errorf("unsupported provider: %s", p)
	}

	if err != nil {
		return nil, err
	}

	m.cache.Store(cacheKey, item)
	return item, nil
}

func providerBucketCacheKey(p string, bucketName string) string {
	return "bucket|" + strings.TrimSpace(provider.Normalize(p, "")) + "|" + strings.TrimSpace(bucketName)
}

func isSigningNotSupported(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "unimplemented") || strings.Contains(lower, "not supported")
}

func (m *Manager) openS3Bucket(ctx context.Context, bucketName string) (*cacheItem, error) {
	cred, err := m.credentialForBucket(ctx, bucketName)
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
		Bucket:      bucket,
		S3Client:    client,
		S3Presigner: s3.NewPresignClient(client),
		Provider:    provider.S3,
		BucketName:  bucketName,
	}, nil
}
