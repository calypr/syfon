package common

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/calypr/syfon/internal/models"
)

const (
	S3Provider    = "s3"
	GCSProvider   = "gcs"
	AzureProvider = "azure"
	FileProvider  = "file"

	S3Prefix    = "s3://"
	GCSPrefix   = "gs://"
	AzurePrefix = "azblob://"
	DRSPrefix   = "drs://"
)

func NormalizeProvider(p string, fallback string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	switch p {
	case S3Provider, GCSProvider, AzureProvider, FileProvider:
		return p
	case "gs":
		return GCSProvider
	case "azblob":
		return AzureProvider
	default:
		if fallback != "" {
			return NormalizeProvider(fallback, "")
		}
		return S3Provider
	}
}

func ProviderFromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSuffix(strings.TrimSpace(scheme), "://")) {
	case "s3":
		return S3Provider
	case "gs":
		return GCSProvider
	case "azblob":
		return AzureProvider
	default:
		return ""
	}
}

func ProviderToScheme(p string) string {
	switch NormalizeProvider(p, "") {
	case S3Provider:
		return "s3"
	case GCSProvider:
		return "gs"
	case AzureProvider:
		return "azblob"
	case FileProvider:
		return "file"
	default:
		return "s3"
	}
}

func ObjectURLForCredential(cred *models.S3Credential, key string) (string, error) {
	if cred == nil {
		return "", fmt.Errorf("credential is required")
	}
	cleanKey := strings.TrimPrefix(strings.TrimSpace(key), "/")
	provider := NormalizeProvider(cred.Provider, S3Provider)

	switch provider {
	case S3Provider:
		return fmt.Sprintf("%s%s/%s", S3Prefix, cred.Bucket, cleanKey), nil
	case GCSProvider:
		return fmt.Sprintf("%s%s/%s", GCSPrefix, cred.Bucket, cleanKey), nil
	case AzureProvider:
		return fmt.Sprintf("%s%s/%s", AzurePrefix, cred.Bucket, cleanKey), nil
	case FileProvider:
		root := filepath.Clean(strings.TrimSpace(cred.Endpoint))
		if root == "." || root == "" {
			root = strings.TrimPrefix(strings.TrimSpace(cred.Bucket), "/")
		}
		if root == "" {
			return "", fmt.Errorf("file provider requires an endpoint or bucket root")
		}
		return filepath.ToSlash(filepath.Join(root, cleanKey)), nil
	default:
		return "", fmt.Errorf("unsupported provider: %s", provider)
	}
}

func NormalizeStoragePath(rawPath, bucket string) (string, error) {
	p := strings.TrimSpace(rawPath)
	if p == "" {
		return "", nil
	}
	u, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("invalid storage path: %w", err)
	}

	targetBucket := strings.TrimSpace(bucket)
	if targetBucket != "" && !strings.EqualFold(strings.TrimSpace(u.Host), targetBucket) {
		return "", fmt.Errorf("path bucket %q does not match expected bucket %q", u.Host, targetBucket)
	}

	if ProviderFromScheme(u.Scheme) == "" {
		return "", fmt.Errorf("unsupported storage scheme: %s", u.Scheme)
	}

	return strings.Trim(strings.TrimSpace(u.Path), "/"), nil
}
