package metrics

import (
	"context"
	"net/http"
	"strings"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/storagemetrics"
)

func (s *MetricsServer) GetStorageSummary(ctx context.Context, request metricsapi.GetStorageSummaryRequestObject) (metricsapi.GetStorageSummaryResponseObject, error) {
	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		return getStorageSummaryAuthResponse(statusCode), nil
	}
	if !access.isScoped() || strings.TrimSpace(access.project) == "" {
		return metricsapi.GetStorageSummary400Response{}, nil
	}
	normalizedPath, _, err := common.NormalizeBrowsePath(pointerString(request.Params.Path))
	if err != nil {
		return metricsapi.GetStorageSummary400Response{}, nil
	}
	summary, err := s.database.GetStoragePathSummary(ctx, access.organization, access.project, normalizedPath)
	if err != nil {
		return metricsapi.GetStorageSummary500Response{}, nil
	}
	return metricsapi.GetStorageSummary200JSONResponse(storageSummaryToGenerated(summary)), nil
}

func (s *MetricsServer) ListStorageChildren(ctx context.Context, request metricsapi.ListStorageChildrenRequestObject) (metricsapi.ListStorageChildrenResponseObject, error) {
	limit := 200
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	offset := 0
	if request.Params.Offset != nil {
		offset = *request.Params.Offset
	}
	if limit < 1 || limit > 1000 || offset < 0 {
		return metricsapi.ListStorageChildren400Response{}, nil
	}
	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		return getStorageChildrenAuthResponse(statusCode), nil
	}
	if !access.isScoped() || strings.TrimSpace(access.project) == "" {
		return metricsapi.ListStorageChildren400Response{}, nil
	}
	normalizedPath, _, err := common.NormalizeBrowsePath(pointerString(request.Params.Path))
	if err != nil {
		return metricsapi.ListStorageChildren400Response{}, nil
	}
	sortBy, sortOrder, err := storagemetrics.NormalizeStorageChildrenSort(pointerString(request.Params.SortBy), pointerString(request.Params.SortOrder))
	if err != nil {
		return metricsapi.ListStorageChildren400Response{}, nil
	}
	items, err := s.database.ListStoragePathChildren(ctx, access.organization, access.project, normalizedPath, limit, offset, sortBy, sortOrder)
	if err != nil {
		return metricsapi.ListStorageChildren500Response{}, nil
	}
	return metricsapi.ListStorageChildren200JSONResponse(storageChildrenToGenerated(access.organization, access.project, normalizedPath, items)), nil
}

func getStorageSummaryAuthResponse(statusCode int) metricsapi.GetStorageSummaryResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.GetStorageSummary401Response{}
	case http.StatusForbidden:
		return metricsapi.GetStorageSummary403Response{}
	default:
		return metricsapi.GetStorageSummary400Response{}
	}
}

func getStorageChildrenAuthResponse(statusCode int) metricsapi.ListStorageChildrenResponseObject {
	switch statusCode {
	case http.StatusUnauthorized:
		return metricsapi.ListStorageChildren401Response{}
	case http.StatusForbidden:
		return metricsapi.ListStorageChildren403Response{}
	default:
		return metricsapi.ListStorageChildren400Response{}
	}
}

func storageSummaryToGenerated(v models.StoragePathSummary) metricsapi.StoragePathSummary {
	return metricsapi.StoragePathSummary{
		Organization:       &v.Organization,
		Project:            &v.Project,
		Path:               &v.Path,
		FileCount:          &v.FileCount,
		RecordCount:        &v.RecordCount,
		TotalBytes:         &v.TotalBytes,
		DownloadCount:      &v.DownloadCount,
		DirectChildCount:   &v.DirectChildCount,
		DuplicatePathCount: &v.DuplicatePathCount,
		LatestUpdateTime:   v.LatestUpdateTime,
		LastDownloadTime:   v.LastDownloadTime,
	}
}

func storageChildrenToGenerated(organization, project, path string, items []models.StoragePathChild) metricsapi.StoragePathChildrenResponse {
	out := metricsapi.StoragePathChildrenResponse{
		Organization: &organization,
		Project:      &project,
		Path:         &path,
	}
	if len(items) == 0 {
		empty := make([]metricsapi.StoragePathChild, 0)
		out.Items = &empty
		return out
	}
	generated := make([]metricsapi.StoragePathChild, 0, len(items))
	for _, item := range items {
		childType := metricsapi.StoragePathChildType(item.Type)
		generated = append(generated, metricsapi.StoragePathChild{
			Name:             &item.Name,
			Path:             &item.Path,
			Type:             &childType,
			FileCount:        &item.FileCount,
			RecordCount:      &item.RecordCount,
			TotalBytes:       &item.TotalBytes,
			LatestUpdateTime: item.LatestUpdateTime,
			DownloadCount:    &item.DownloadCount,
			LastDownloadTime: item.LastDownloadTime,
		})
	}
	out.Items = &generated
	return out
}

func pointerString[T ~string](v *T) string {
	if v == nil {
		return ""
	}
	return string(*v)
}
