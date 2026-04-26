package common

import (
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
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

var (
	s3BucketNameRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	azureBucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	gcsBucketNameRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)
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
	case "gs", "gcs":
		return GCSProvider
	case "az", "azblob":
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

// ParseBucketProvider returns a canonical bucket provider name or an error for
// unsupported values.
func ParseBucketProvider(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", S3Provider:
		return S3Provider, nil
	case GCSProvider, "gs":
		return GCSProvider, nil
	case AzureProvider, "azblob":
		return AzureProvider, nil
	default:
		return "", fmt.Errorf("unsupported provider %q", raw)
	}
}

// ValidateBucketName validates a bucket/container name for the given provider.
//
// The rules are intentionally provider-specific:
// - s3 and azure share the stricter DNS-style naming rules.
// - gcs permits dots and underscores but still requires a DNS-safe shape.
func ValidateBucketName(providerName, bucketName string) error {
	bucketName = strings.TrimSpace(bucketName)
	if bucketName == "" {
		return fmt.Errorf("bucket name is required")
	}

	switch NormalizeProvider(providerName, "") {
	case S3Provider:
		return validateS3BucketName(bucketName)
	case GCSProvider:
		return validateGCSBucketName(bucketName)
	case AzureProvider:
		return validateAzureBucketName(bucketName)
	default:
		return fmt.Errorf("unsupported provider %q", providerName)
	}
}

func validateS3BucketName(bucketName string) error {
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return fmt.Errorf("bucket name %q must be 3-63 characters", bucketName)
	}
	if !s3BucketNameRE.MatchString(bucketName) {
		return fmt.Errorf("bucket name %q is invalid (lowercase letters, numbers, hyphens only; must start and end with letter or number)", bucketName)
	}
	return nil
}

func validateAzureBucketName(bucketName string) error {
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return fmt.Errorf("bucket name %q must be 3-63 characters", bucketName)
	}
	if strings.Contains(bucketName, "--") {
		return fmt.Errorf("bucket name %q is invalid (consecutive hyphens are not allowed)", bucketName)
	}
	if !azureBucketNameRE.MatchString(bucketName) {
		return fmt.Errorf("bucket name %q is invalid (lowercase letters, numbers, hyphens only; must start and end with letter or number)", bucketName)
	}
	return nil
}

func validateGCSBucketName(bucketName string) error {
	if len(bucketName) < 3 {
		return fmt.Errorf("bucket name %q must be at least 3 characters", bucketName)
	}
	if strings.ContainsAny(bucketName, " \t\n\r") {
		return fmt.Errorf("bucket name %q is invalid (spaces are not allowed)", bucketName)
	}
	if !gcsBucketNameRE.MatchString(bucketName) {
		return fmt.Errorf("bucket name %q is invalid (lowercase letters, numbers, hyphens, underscores, and dots only; must start and end with letter or number)", bucketName)
	}
	if strings.HasPrefix(bucketName, "goog") {
		return fmt.Errorf("bucket name %q is invalid (cannot begin with \"goog\")", bucketName)
	}
	if net.ParseIP(bucketName) != nil {
		return fmt.Errorf("bucket name %q is invalid (cannot be an IP address)", bucketName)
	}
	if strings.Contains(bucketName, ".") {
		if len(bucketName) > 222 {
			return fmt.Errorf("bucket name %q must be 222 characters or fewer when dots are present", bucketName)
		}
		for _, segment := range strings.Split(bucketName, ".") {
			if segment == "" || len(segment) > 63 {
				return fmt.Errorf("bucket name %q is invalid (each dot-separated component must be 1-63 characters)", bucketName)
			}
		}
	} else if len(bucketName) > 63 {
		return fmt.Errorf("bucket name %q must be 3-63 characters", bucketName)
	}
	return nil
}
