package bucket

import (
	"fmt"
	"net/url"
	"strings"
)

// ParseStorageLocation extracts the bucket and key from a raw storage URL (e.g., s3://bucket/key).
func ParseStorageLocation(rawURL string) (bucketName, key string, err error) {
	if rawURL == "" {
		return "", "", fmt.Errorf("empty storage URL")
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse storage URL %q: %w", rawURL, err)
	}
	bucketName = strings.TrimSpace(parsed.Host)
	key = strings.Trim(strings.TrimSpace(parsed.Path), "/")
	if bucketName == "" || key == "" {
		return "", "", fmt.Errorf("invalid storage location %q: missing bucket or key", rawURL)
	}
	return bucketName, key, nil
}
