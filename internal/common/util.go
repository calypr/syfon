package common

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CleanToBasename extracts the basename from a path (handling both windows and unix separators).
func CleanToBasename(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	base := filepath.Base(trimmed)
	if base == "." || base == "/" || base == "" {
		base = trimmed
	}
	return base
}



// SchemeFromURL extracts the scheme from a URL string.
func SchemeFromURL(raw string) string {
	if i := strings.Index(raw, "://"); i != -1 {
		return strings.ToLower(raw[:i])
	}
	return ""
}



// BucketToURL converts a bucket and key to an s3:// URL.
func BucketToURL(bucket, key string) string {
	return fmt.Sprintf("s3://%s/%s", strings.TrimPrefix(bucket, "s3://"), strings.TrimPrefix(key, "/"))
}
