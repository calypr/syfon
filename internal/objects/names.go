package objects

import (
	"sort"
	"strings"
)

// CleanToBasename extracts a portable basename from either Windows or Unix
// path syntax.
func CleanToBasename(name string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	parts := strings.Split(trimmed, "/")
	base := parts[len(parts)-1]
	if base == "" {
		return trimmed
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
