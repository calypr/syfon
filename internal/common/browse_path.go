package common

import (
	"fmt"
	"strings"
)

type BrowsePathInfo struct {
	Normalized string
	ParentPath string
	EntryName  string
	Segments   []string
}

func NormalizeBrowsePath(raw string) (string, []string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil, nil
	}
	trimmed = strings.ReplaceAll(trimmed, "\\", "/")
	parts := strings.Split(trimmed, "/")
	segments := make([]string, 0, len(parts))
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			continue
		}
		if segment == "." || segment == ".." {
			return "", nil, fmt.Errorf("path must not contain '.' or '..' segments")
		}
		segments = append(segments, segment)
	}
	return strings.Join(segments, "/"), segments, nil
}

func BrowsePathInfoFromName(raw string) (BrowsePathInfo, bool, error) {
	normalized, segments, err := NormalizeBrowsePath(raw)
	if err != nil {
		return BrowsePathInfo{}, false, err
	}
	if normalized == "" || len(segments) == 0 {
		return BrowsePathInfo{}, false, nil
	}
	parentPath := ""
	if len(segments) > 1 {
		parentPath = strings.Join(segments[:len(segments)-1], "/")
	}
	return BrowsePathInfo{
		Normalized: normalized,
		ParentPath: parentPath,
		EntryName:  segments[len(segments)-1],
		Segments:   segments,
	}, true, nil
}

func HasBrowsePathPrefix(candidate, prefix []string) bool {
	if len(prefix) > len(candidate) {
		return false
	}
	for i, segment := range prefix {
		if candidate[i] != segment {
			return false
		}
	}
	return true
}

func ImmediateBrowseDirectory(requestedPath, candidatePath string) (BrowsePathInfo, bool) {
	requestedNormalized, requestedSegments, err := NormalizeBrowsePath(requestedPath)
	if err != nil {
		return BrowsePathInfo{}, false
	}
	candidateInfo, ok, err := BrowsePathInfoFromName(candidatePath)
	if err != nil || !ok {
		return BrowsePathInfo{}, false
	}
	if !HasBrowsePathPrefix(candidateInfo.Segments, requestedSegments) {
		return BrowsePathInfo{}, false
	}
	remaining := candidateInfo.Segments[len(requestedSegments):]
	if len(remaining) < 2 {
		return BrowsePathInfo{}, false
	}
	dirSegments := append(append([]string{}, requestedSegments...), remaining[0])
	dirPath := strings.Join(dirSegments, "/")
	parentPath := ""
	if requestedNormalized != "" {
		parentPath = requestedNormalized
	}
	return BrowsePathInfo{
		Normalized: dirPath,
		ParentPath: parentPath,
		EntryName:  remaining[0],
		Segments:   dirSegments,
	}, true
}
