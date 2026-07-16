package common

import (
	"fmt"
	"path/filepath"
	"sort"
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

func NormalizeNameAliases(primary string, aliases []string) []string {
	primary = CleanToBasename(primary)
	seen := make(map[string]struct{}, len(aliases)+1)
	out := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		name := CleanToBasename(alias)
		if name == "" || name == primary {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
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
