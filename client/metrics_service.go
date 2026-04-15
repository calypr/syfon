package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/request"
)

type MetricsService struct {
	requestor request.Requester
}

func NewMetricsService(r request.Requester) *MetricsService {
	return &MetricsService{requestor: r}
}

func (s *MetricsService) Summary(ctx context.Context, opts MetricsSummaryOptions) (map[string]any, error) {
	q := url.Values{}
	if opts.InactiveDays > 0 {
		q.Set(common.QueryParamInactiveDays, strconv.Itoa(opts.InactiveDays))
	}
	if opts.Organization != "" {
		q.Set(common.QueryParamOrganization, opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set(common.QueryParamProject, opts.ProjectID)
	}
	var out map[string]any
	err := s.requestor.Do(ctx, http.MethodGet, common.MetricsSummaryEndpoint, nil, &out, request.WithQueryValues(q))
	return out, err
}

func (s *MetricsService) Files(ctx context.Context, opts MetricsFilesOptions) ([]map[string]any, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set(common.QueryParamLimit, strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		q.Set(common.QueryParamOffset, strconv.Itoa(opts.Offset))
	}
	if opts.InactiveDays > 0 {
		q.Set(common.QueryParamInactiveDays, strconv.Itoa(opts.InactiveDays))
	}
	if opts.Organization != "" {
		q.Set(common.QueryParamOrganization, opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set(common.QueryParamProject, opts.ProjectID)
	}
	var out struct {
		Data *[]map[string]any `json:"data"`
	}
	err := s.requestor.Do(ctx, http.MethodGet, common.MetricsFilesEndpoint, nil, &out, request.WithQueryValues(q))
	if err != nil {
		return nil, err
	}
	if out.Data == nil {
		return []map[string]any{}, nil
	}
	return *out.Data, nil
}

func (s *MetricsService) File(ctx context.Context, objectID string, opts MetricsFilesOptions) (map[string]any, error) {
	q := url.Values{}
	if opts.Organization != "" {
		q.Set(common.QueryParamOrganization, opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set(common.QueryParamProject, opts.ProjectID)
	}
	out := map[string]any{}
	err := s.requestor.Do(ctx, http.MethodGet, fmt.Sprintf(common.MetricsFileEndpointTemplate, url.PathEscape(objectID)), nil, &out, request.WithQueryValues(q))
	return out, err
}
