package storagemetrics

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
)

const (
	StoragePathChildTypeFile      = "file"
	StoragePathChildTypeDirectory = "directory"
)

func AggregateStoragePathSummary(organization, project, path string, rows []models.DrsObjectRecord) (models.StoragePathSummary, error) {
	normalizedPath, requestedSegments, err := common.NormalizeBrowsePath(path)
	if err != nil {
		return models.StoragePathSummary{}, err
	}
	summary := models.StoragePathSummary{
		Organization: organization,
		Project:      project,
		Path:         normalizedPath,
	}
	distinctPaths := make(map[string]struct{})
	directChildren := make(map[string]struct{})
	recordsByPath := make(map[string]int64)
	for _, row := range rows {
		info, ok := storageMetricPathInfo(row)
		if !ok || !common.HasBrowsePathPrefix(info.Segments, requestedSegments) {
			continue
		}
		if len(info.Segments) > len(requestedSegments) {
			childPath := strings.Join(info.Segments[:len(requestedSegments)+1], "/")
			directChildren[childPath] = struct{}{}
		}
		summary.RecordCount++
		summary.TotalBytes += row.Size
		summary.DownloadCount += row.DownloadCount
		distinctPaths[info.Normalized] = struct{}{}
		recordsByPath[info.Normalized]++
		if row.LastDownloadTime != nil && (summary.LastDownloadTime == nil || row.LastDownloadTime.After(*summary.LastDownloadTime)) {
			t := row.LastDownloadTime.UTC()
			summary.LastDownloadTime = &t
		}
		if row.UpdatedTime.IsZero() {
			continue
		}
		if summary.LatestUpdateTime == nil || row.UpdatedTime.After(*summary.LatestUpdateTime) {
			t := row.UpdatedTime.UTC()
			summary.LatestUpdateTime = &t
		}
	}
	summary.FileCount = int64(len(distinctPaths))
	summary.DirectChildCount = int64(len(directChildren))
	for _, count := range recordsByPath {
		if count > 1 {
			summary.DuplicatePathCount++
		}
	}
	return summary, nil
}

func AggregateStoragePathChildren(path string, rows []models.DrsObjectRecord, limit, offset int, sortBy, sortOrder string) ([]models.StoragePathChild, error) {
	_, requestedSegments, err := common.NormalizeBrowsePath(path)
	if err != nil {
		return nil, err
	}
	type childAggregate struct {
		item          models.StoragePathChild
		distinctPaths map[string]struct{}
		isDirectory   bool
	}
	children := make(map[string]*childAggregate)
	for _, row := range rows {
		info, ok := storageMetricPathInfo(row)
		if !ok || !common.HasBrowsePathPrefix(info.Segments, requestedSegments) || len(info.Segments) <= len(requestedSegments) {
			continue
		}
		childSegments := info.Segments[:len(requestedSegments)+1]
		childPath := strings.Join(childSegments, "/")
		agg, exists := children[childPath]
		if !exists {
			agg = &childAggregate{
				item: models.StoragePathChild{
					Name: childSegments[len(childSegments)-1],
					Path: childPath,
					Type: StoragePathChildTypeFile,
				},
				distinctPaths: make(map[string]struct{}),
			}
			children[childPath] = agg
		}
		if len(info.Segments) > len(requestedSegments)+1 {
			agg.isDirectory = true
			agg.item.Type = StoragePathChildTypeDirectory
		}
		agg.item.RecordCount++
		agg.item.TotalBytes += row.Size
		agg.item.DownloadCount += row.DownloadCount
		agg.distinctPaths[info.Normalized] = struct{}{}
		if row.LastDownloadTime != nil && (agg.item.LastDownloadTime == nil || row.LastDownloadTime.After(*agg.item.LastDownloadTime)) {
			t := row.LastDownloadTime.UTC()
			agg.item.LastDownloadTime = &t
		}
		if row.UpdatedTime.IsZero() {
			continue
		}
		if agg.item.LatestUpdateTime == nil || row.UpdatedTime.After(*agg.item.LatestUpdateTime) {
			t := row.UpdatedTime.UTC()
			agg.item.LatestUpdateTime = &t
		}
	}
	items := make([]models.StoragePathChild, 0, len(children))
	for _, agg := range children {
		agg.item.FileCount = int64(len(agg.distinctPaths))
		if agg.isDirectory {
			agg.item.Type = StoragePathChildTypeDirectory
		}
		items = append(items, agg.item)
	}
	sortStoragePathChildren(items, sortBy, sortOrder)
	if offset < 0 {
		offset = 0
	}
	if offset >= len(items) {
		return []models.StoragePathChild{}, nil
	}
	if limit <= 0 {
		return items[offset:], nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	return items[offset:end], nil
}

func storageMetricPathInfo(row models.DrsObjectRecord) (common.BrowsePathInfo, bool) {
	raw := strings.TrimSpace(row.Path)
	if raw == "" {
		raw = strings.TrimSpace(row.Name)
	}
	info, ok, err := common.BrowsePathInfoFromName(raw)
	if err != nil || !ok {
		return common.BrowsePathInfo{}, false
	}
	return info, true
}

func sortStoragePathChildren(items []models.StoragePathChild, sortBy, sortOrder string) {
	desc := strings.EqualFold(strings.TrimSpace(sortOrder), "desc")
	key := strings.TrimSpace(sortBy)
	if key == "" {
		key = "name"
	}
	sort.Slice(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		less := false
		switch key {
		case "bytes":
			less = left.TotalBytes < right.TotalBytes
		case "updated_time":
			less = compareTimePtr(left.LatestUpdateTime, right.LatestUpdateTime)
		case "records":
			less = left.RecordCount < right.RecordCount
		case "name":
			fallthrough
		default:
			less = strings.ToLower(left.Name) < strings.ToLower(right.Name)
		}
		if equivalentSortValue(left, right, key) {
			if strings.EqualFold(left.Name, right.Name) {
				if desc {
					return left.Path > right.Path
				}
				return left.Path < right.Path
			}
			if desc {
				return strings.ToLower(left.Name) > strings.ToLower(right.Name)
			}
			return strings.ToLower(left.Name) < strings.ToLower(right.Name)
		}
		if desc {
			return !less
		}
		return less
	})
}

func equivalentSortValue(left, right models.StoragePathChild, key string) bool {
	switch key {
	case "bytes":
		return left.TotalBytes == right.TotalBytes
	case "updated_time":
		return sameTimePtr(left.LatestUpdateTime, right.LatestUpdateTime)
	case "records":
		return left.RecordCount == right.RecordCount
	default:
		return strings.EqualFold(left.Name, right.Name)
	}
}

func compareTimePtr(left, right *time.Time) bool {
	if left == nil && right == nil {
		return false
	}
	if left == nil {
		return true
	}
	if right == nil {
		return false
	}
	return left.Before(*right)
}

func sameTimePtr(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Equal(*right)
}

func NormalizeStorageChildrenSort(sortBy, sortOrder string) (string, string, error) {
	sortBy = strings.TrimSpace(sortBy)
	if sortBy == "" {
		sortBy = "name"
	}
	switch sortBy {
	case "bytes", "name", "updated_time", "records":
	default:
		return "", "", fmt.Errorf("invalid sort_by")
	}
	sortOrder = strings.TrimSpace(sortOrder)
	if sortOrder == "" {
		sortOrder = "asc"
	}
	switch sortOrder {
	case "asc", "desc":
	default:
		return "", "", fmt.Errorf("invalid sort_order")
	}
	return sortBy, sortOrder, nil
}
