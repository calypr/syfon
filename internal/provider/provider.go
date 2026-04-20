package provider

import (
	"fmt"
	"net"
	"regexp"
	"strings"
)

const (
	S3    = "s3"
	GCS   = "gcs"
	Azure = "azure"
)

var (
	s3BucketNameRE    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	azureBucketNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$`)
	gcsBucketNameRE   = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)
)

// Normalize returns a standard provider name from a raw string.
func Normalize(p string, fallback string) string {
	p = strings.ToLower(strings.TrimSpace(p))
	switch p {
	case S3, GCS, Azure:
		return p
	case "gs":
		return GCS
	case "azblob":
		return Azure
	default:
		if fallback != "" {
			return Normalize(fallback, "")
		}
		return S3
	}
}

// FromScheme maps a URL scheme to a provider name.
func FromScheme(scheme string) string {
	switch strings.ToLower(strings.TrimSpace(scheme)) {
	case "s3":
		return S3
	case "gs":
		return GCS
	case "azblob":
		return Azure
	default:
		return ""
	}
}

// ToScheme maps a provider name to its primary URL scheme.
func ToScheme(p string) string {
	switch Normalize(p, "") {
	case S3:
		return "s3"
	case GCS:
		return "gs"
	case Azure:
		return "azblob"
	default:
		return "s3"
	}
}

// ParseBucketProvider returns a canonical bucket provider name or an error for
// unsupported values.
func ParseBucketProvider(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", S3:
		return S3, nil
	case GCS, "gs":
		return GCS, nil
	case Azure, "azblob":
		return Azure, nil
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

	switch strings.ToLower(strings.TrimSpace(providerName)) {
	case S3:
		return validateS3BucketName(bucketName)
	case GCS, "gs":
		return validateGCSBucketName(bucketName)
	case Azure, "azblob":
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
