package urlmanager

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/calypr/drs-server/db/core"
)

// RoutedUrlManager routes signing and multipart requests by provider metadata
// stored on bucket credentials.
type RoutedUrlManager struct {
	database            core.DatabaseInterface
	defaultProvider     string
	signedURLByProvider map[string]SignedURLManager
	multipartByProvider map[string]MultipartManager
}

func NewRoutedUrlManager(database core.DatabaseInterface) *RoutedUrlManager {
	s3 := NewS3UrlManager(database)
	blob := NewBlobUrlManager()

	return &RoutedUrlManager{
		database:        database,
		defaultProvider: "s3",
		signedURLByProvider: map[string]SignedURLManager{
			"s3":    s3,
			"gcs":   blob,
			"azure": blob,
			"file":  blob,
		},
		multipartByProvider: map[string]MultipartManager{
			"s3": s3,
		},
	}
}

func (m *RoutedUrlManager) SignURL(ctx context.Context, accessId string, rawURL string, opts SignOptions) (string, error) {
	provider, err := m.resolveProvider(ctx, accessId, rawURL)
	if err != nil {
		return "", err
	}
	signer, ok := m.signedURLByProvider[provider]
	if !ok {
		return "", fmt.Errorf("unsupported provider for signed URL: %s", provider)
	}
	return signer.SignURL(ctx, accessId, rawURL, opts)
}

func (m *RoutedUrlManager) SignUploadURL(ctx context.Context, accessId string, rawURL string, opts SignOptions) (string, error) {
	provider, err := m.resolveProvider(ctx, accessId, rawURL)
	if err != nil {
		return "", err
	}
	signer, ok := m.signedURLByProvider[provider]
	if !ok {
		return "", fmt.Errorf("unsupported provider for signed upload URL: %s", provider)
	}
	return signer.SignUploadURL(ctx, accessId, rawURL, opts)
}

func (m *RoutedUrlManager) InitMultipartUpload(ctx context.Context, bucket string, key string) (string, error) {
	provider, err := m.resolveProviderForBucket(ctx, bucket)
	if err != nil {
		return "", err
	}
	mp, ok := m.multipartByProvider[provider]
	if !ok {
		return "", fmt.Errorf("multipart is not supported for provider: %s", provider)
	}
	return mp.InitMultipartUpload(ctx, bucket, key)
}

func (m *RoutedUrlManager) SignMultipartPart(ctx context.Context, bucket string, key string, uploadId string, partNumber int32) (string, error) {
	provider, err := m.resolveProviderForBucket(ctx, bucket)
	if err != nil {
		return "", err
	}
	mp, ok := m.multipartByProvider[provider]
	if !ok {
		return "", fmt.Errorf("multipart is not supported for provider: %s", provider)
	}
	return mp.SignMultipartPart(ctx, bucket, key, uploadId, partNumber)
}

func (m *RoutedUrlManager) CompleteMultipartUpload(ctx context.Context, bucket string, key string, uploadId string, parts []MultipartPart) error {
	provider, err := m.resolveProviderForBucket(ctx, bucket)
	if err != nil {
		return err
	}
	mp, ok := m.multipartByProvider[provider]
	if !ok {
		return fmt.Errorf("multipart is not supported for provider: %s", provider)
	}
	return mp.CompleteMultipartUpload(ctx, bucket, key, uploadId, parts)
}

func (m *RoutedUrlManager) resolveProvider(ctx context.Context, accessId string, rawURL string) (string, error) {
	if bucket := strings.TrimSpace(accessId); bucket != "" {
		return m.resolveProviderForBucket(ctx, bucket)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse object URL: %w", err)
	}
	bucket := strings.TrimSpace(parsed.Host)
	if bucket != "" {
		if p, err := m.resolveProviderForBucket(ctx, bucket); err == nil {
			return p, nil
		}
	}
	return normalizeProvider(providerFromScheme(parsed.Scheme), m.defaultProvider), nil
}

func (m *RoutedUrlManager) resolveProviderForBucket(ctx context.Context, bucket string) (string, error) {
	cred, err := m.database.GetS3Credential(ctx, strings.TrimSpace(bucket))
	if err != nil {
		return "", err
	}
	return normalizeProvider(cred.Provider, m.defaultProvider), nil
}

func normalizeProvider(provider, fallback string) string {
	p := strings.ToLower(strings.TrimSpace(provider))
	if p == "" {
		p = strings.ToLower(strings.TrimSpace(fallback))
	}
	if p == "" {
		return "s3"
	}
	return p
}

func providerFromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "s3":
		return "s3"
	case "gs":
		return "gcs"
	case "azblob":
		return "azure"
	case "file":
		return "file"
	default:
		return ""
	}
}
