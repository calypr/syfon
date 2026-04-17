package common

import (
	"fmt"
	"strings"
)

// UniqueStrings returns a deduped slice of strings, preserving order.
func UniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

// UniqueStringsCaseInsensitive returns a deduped slice of strings based on lowercase comparison, preserving the first-seen original string.
func UniqueStringsCaseInsensitive(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, v := range values {
		normalized := strings.ToLower(strings.TrimSpace(v))
		if normalized == "" {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, v)
	}
	return out
}

// SchemeFromURL extracts the scheme from a URL string.
func SchemeFromURL(raw string) string {
	if i := strings.Index(raw, "://"); i != -1 {
		return strings.ToLower(raw[:i])
	}
	return ""
}

// NormalizeUploadKey ensures a key is valid for upload and defaults to ID if empty.
func NormalizeUploadKey(inputKey, id string) string {
	k := strings.TrimSpace(inputKey)
	if k == "" {
		return id
	}
	return k
}

// BucketToURL converts a bucket and key to an s3:// URL.
func BucketToURL(bucket, key string) string {
	return fmt.Sprintf("s3://%s/%s", strings.TrimPrefix(bucket, "s3://"), strings.TrimPrefix(key, "/"))
}
