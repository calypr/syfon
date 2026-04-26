package metrics

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	sycommon "github.com/calypr/syfon/common"
	"github.com/calypr/syfon/internal/authz"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/gofiber/fiber/v3"
)

type metricsQueryContextKey struct{}

type metricsQueryParams struct {
	organization string
	program      string
	project      string
}

type MetricsServer struct {
	database db.MetricsStore
}

func NewMetricsServer(database db.MetricsStore) *MetricsServer {
	return &MetricsServer{database: database}
}

func RegisterMetricsRoutes(router fiber.Router, database db.MetricsStore) {
	router.Use(func(c fiber.Ctx) error {
		params := metricsQueryParams{
			organization: strings.TrimSpace(c.Query("organization")),
			program:      strings.TrimSpace(c.Query("program")),
			project:      strings.TrimSpace(c.Query("project")),
		}
		c.SetContext(context.WithValue(c.Context(), metricsQueryContextKey{}, params))
		return c.Next()
	})

	server := NewMetricsServer(database)
	strict := metricsapi.NewStrictHandler(server, nil)
	metricsapi.RegisterHandlers(router, strict)
}

func (s *MetricsServer) ListMetricsFiles(ctx context.Context, request metricsapi.ListMetricsFilesRequestObject) (metricsapi.ListMetricsFilesResponseObject, error) {
	limit := 200
	if request.Params.Limit != nil {
		limit = *request.Params.Limit
	}
	offset := 0
	if request.Params.Offset != nil {
		offset = *request.Params.Offset
	}

	if limit < 1 || limit > 1000 || offset < 0 {
		return metricsapi.ListMetricsFiles400Response{}, nil
	}

	inactiveSince, err := parseInactiveSince(request.Params.InactiveDays)
	if err != nil {
		return metricsapi.ListMetricsFiles400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.ListMetricsFiles401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.ListMetricsFiles403Response{}, nil
		default:
			return metricsapi.ListMetricsFiles400Response{}, nil
		}
	}

	var data []models.FileUsage
	if access.isScoped() {
		data, _, err = listScopedFileUsage(ctx, s.database, access.organization, access.project, limit, offset, inactiveSince)
	} else {
		data, err = s.database.ListFileUsage(ctx, limit, offset, inactiveSince)
	}
	if err != nil {
		return metricsapi.ListMetricsFiles500Response{}, nil
	}

	items := make([]metricsapi.FileUsage, 0, len(data))
	for _, v := range data {
		items = append(items, toMetricsFileUsage(v))
	}

	return metricsapi.ListMetricsFiles200JSONResponse{
		Data:   &items,
		Limit:  &limit,
		Offset: &offset,
	}, nil
}

func (s *MetricsServer) GetMetricsFile(ctx context.Context, request metricsapi.GetMetricsFileRequestObject) (metricsapi.GetMetricsFileResponseObject, error) {
	objectID := request.ObjectId
	if objectID == "" {
		return metricsapi.GetMetricsFile400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.GetMetricsFile401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.GetMetricsFile403Response{}, nil
		default:
			return metricsapi.GetMetricsFile400Response{}, nil
		}
	}

	if access.isScoped() {
		inside, err := objectInScope(ctx, s.database, objectID, access.organization, access.project)
		if err != nil {
			return metricsapi.GetMetricsFile500Response{}, nil
		}
		if !inside {
			return metricsapi.GetMetricsFile404Response{}, nil
		}
	}

	usage, err := s.database.GetFileUsage(ctx, objectID)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return metricsapi.GetMetricsFile404Response{}, nil
		}
		return metricsapi.GetMetricsFile500Response{}, nil
	}

	return metricsapi.GetMetricsFile200JSONResponse(toMetricsFileUsage(*usage)), nil
}

func (s *MetricsServer) GetMetricsSummary(ctx context.Context, request metricsapi.GetMetricsSummaryRequestObject) (metricsapi.GetMetricsSummaryResponseObject, error) {
	inactiveSince, err := parseInactiveSince(request.Params.InactiveDays)
	if err != nil {
		return metricsapi.GetMetricsSummary400Response{}, nil
	}

	access, statusCode, ok := s.checkAuth(ctx)
	if !ok {
		switch statusCode {
		case http.StatusUnauthorized:
			return metricsapi.GetMetricsSummary401Response{}, nil
		case http.StatusForbidden:
			return metricsapi.GetMetricsSummary403Response{}, nil
		default:
			return metricsapi.GetMetricsSummary400Response{}, nil
		}
	}

	var summary models.FileUsageSummary
	if access.isScoped() {
		_, summary, err = listScopedFileUsage(ctx, s.database, access.organization, access.project, 0, 0, inactiveSince)
	} else {
		summary, err = s.database.GetFileUsageSummary(ctx, inactiveSince)
	}
	if err != nil {
		return metricsapi.GetMetricsSummary500Response{}, nil
	}

	return metricsapi.GetMetricsSummary200JSONResponse{
		TotalFiles:        &summary.TotalFiles,
		TotalUploads:      &summary.TotalUploads,
		TotalDownloads:    &summary.TotalDownloads,
		InactiveFileCount: &summary.InactiveFileCount,
	}, nil
}

func (s *MetricsServer) checkAuth(ctx context.Context) (metricsAccess, int, bool) {
	access, err := resolveMetricsAccess(ctx)
	if err != nil {
		return metricsAccess{}, http.StatusBadRequest, false
	}

	if !authz.IsGen3Mode(ctx) {
		return access, 0, true
	}
	if !authz.HasAuthHeader(ctx) {
		return access, http.StatusUnauthorized, false
	}

	// Baseline read access for metrics: global access or scoped access
	if authz.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		authz.HasMethodAccess(ctx, "read", []string{"/programs"}) {
		return access, 0, true
	}

	if access.isScoped() {
		scope, err := sycommon.ResourcePath(access.organization, access.project)
		if err != nil {
			return access, http.StatusBadRequest, false
		}
		if authz.HasMethodAccess(ctx, "read", []string{scope}) {
			return access, 0, true
		}
	}

	return access, http.StatusForbidden, false
}

func toMetricsFileUsage(v models.FileUsage) metricsapi.FileUsage {
	return metricsapi.FileUsage{
		ObjectId:         &v.ObjectID,
		Name:             &v.Name,
		Size:             &v.Size,
		UploadCount:      &v.UploadCount,
		DownloadCount:    &v.DownloadCount,
		LastUploadTime:   v.LastUploadTime,
		LastDownloadTime: v.LastDownloadTime,
		LastAccessTime:   v.LastAccessTime,
	}
}

func parseInactiveSince(inactiveDays *int) (*time.Time, error) {
	if inactiveDays == nil {
		return nil, nil
	}
	days := *inactiveDays
	if days < 0 {
		return nil, errors.New("inactive_days must be a non-negative integer")
	}
	t := time.Now().UTC().AddDate(0, 0, -days)
	return &t, nil
}

type metricsAccess struct {
	organization string
	project      string
}

func (a metricsAccess) isScoped() bool {
	return strings.TrimSpace(a.organization) != ""
}

func resolveMetricsAccess(ctx context.Context) (metricsAccess, error) {
	org, project, _, err := parseScopeQuery(ctx)
	if err != nil {
		return metricsAccess{}, err
	}
	return metricsAccess{organization: org, project: project}, nil
}

func hasGlobalMetricsReadAccess(ctx context.Context) bool {
	return authz.HasMethodAccess(ctx, "read", []string{"/data_file"}) ||
		authz.HasMethodAccess(ctx, "read", []string{"/programs"})
}

func parseScopeQuery(ctx context.Context) (string, string, bool, error) {
	params, _ := ctx.Value(metricsQueryContextKey{}).(metricsQueryParams)
	org := strings.TrimSpace(params.organization)
	if org == "" {
		org = strings.TrimSpace(params.program)
	}
	project := strings.TrimSpace(params.project)
	if project != "" && org == "" {
		return "", "", false, errors.New("organization is required when project is set")
	}
	if org != "" {
		return org, project, true, nil
	}
	return "", "", false, nil
}

func collectScopedUsage(ctx context.Context, database db.MetricsStore, organization, project string, inactiveSince *time.Time) ([]models.FileUsage, models.FileUsageSummary, error) {
	ids, err := database.ListObjectIDsByScope(ctx, organization, project)
	if err != nil {
		return nil, models.FileUsageSummary{}, err
	}
	sort.Strings(ids)

	summary := models.FileUsageSummary{TotalFiles: int64(len(ids))}
	usages := make([]models.FileUsage, 0, len(ids))
	for _, id := range ids {
		usage, err := database.GetFileUsage(ctx, id)
		if err != nil {
			if errors.Is(err, common.ErrNotFound) {
				if inactiveSince != nil {
					summary.InactiveFileCount++
				}
				obj, objErr := database.GetObject(ctx, id)
				if objErr != nil {
					if errors.Is(objErr, common.ErrNotFound) {
						continue
					}
					return nil, models.FileUsageSummary{}, objErr
				}
				usages = append(usages, models.FileUsage{
					ObjectID: id,
					Name:     common.StringVal(obj.Name),
					Size:     obj.Size,
				})
				continue
			}
			return nil, models.FileUsageSummary{}, err
		}
		summary.TotalUploads += usage.UploadCount
		summary.TotalDownloads += usage.DownloadCount
		if inactiveSince != nil && (usage.LastDownloadTime == nil || usage.LastDownloadTime.Before(*inactiveSince)) {
			summary.InactiveFileCount++
		}
		if inactiveSince != nil && usage.LastDownloadTime != nil && !usage.LastDownloadTime.Before(*inactiveSince) {
			continue
		}
		usages = append(usages, *usage)
	}
	return usages, summary, nil
}

func listScopedFileUsage(ctx context.Context, database db.MetricsStore, organization, project string, limit, offset int, inactiveSince *time.Time) ([]models.FileUsage, models.FileUsageSummary, error) {
	usages, summary, err := collectScopedUsage(ctx, database, organization, project, inactiveSince)
	if err != nil {
		return nil, models.FileUsageSummary{}, err
	}
	if limit <= 0 {
		return usages, summary, nil
	}
	if offset >= len(usages) {
		return []models.FileUsage{}, summary, nil
	}
	end := offset + limit
	if end > len(usages) {
		end = len(usages)
	}
	return usages[offset:end], summary, nil
}

func objectInScope(ctx context.Context, database db.MetricsStore, objectID, organization, project string) (bool, error) {
	obj, err := database.GetObject(ctx, objectID)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if strings.TrimSpace(organization) == "" {
		return true, nil
	}
	projects, ok := obj.Authorizations[organization]
	if !ok {
		return false, nil
	}
	if strings.TrimSpace(project) == "" || len(projects) == 0 {
		return true, nil
	}
	for _, p := range projects {
		if p == project {
			return true, nil
		}
	}
	return false, nil
}
