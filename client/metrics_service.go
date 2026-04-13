package client

import (
	"context"
	"net/url"
	"strconv"
)

type MetricsService struct {
	base *baseService
}

func (s *MetricsService) Summary(ctx context.Context, opts MetricsSummaryOptions) (map[string]any, error) {
	q := url.Values{}
	if opts.InactiveDays > 0 {
		q.Set("inactive_days", strconv.Itoa(opts.InactiveDays))
	}
	if opts.Organization != "" {
		q.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set("project", opts.ProjectID)
	}
	out := map[string]any{}
	rb := s.base.requestor.New("GET", "/index/v1/metrics/summary").WithQueryValues(q)
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *MetricsService) Files(ctx context.Context, opts MetricsFilesOptions) (map[string]any, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Offset > 0 {
		q.Set("offset", strconv.Itoa(opts.Offset))
	}
	if opts.InactiveDays > 0 {
		q.Set("inactive_days", strconv.Itoa(opts.InactiveDays))
	}
	if opts.Organization != "" {
		q.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set("project", opts.ProjectID)
	}
	out := map[string]any{}
	rb := s.base.requestor.New("GET", "/index/v1/metrics/files").WithQueryValues(q)
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}

func (s *MetricsService) File(ctx context.Context, objectID string, opts MetricsFilesOptions) (map[string]any, error) {
	q := url.Values{}
	if opts.Organization != "" {
		q.Set("organization", opts.Organization)
	}
	if opts.ProjectID != "" {
		q.Set("project", opts.ProjectID)
	}
	out := map[string]any{}
	rb := s.base.requestor.New("GET", "/index/v1/metrics/files/"+url.PathEscape(objectID)).WithQueryValues(q)
	err := s.base.requestor.DoJSON(ctx, rb, &out)
	return out, err
}
