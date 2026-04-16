package syfonclient

import (
	"context"
	"fmt"

	"github.com/calypr/syfon/apigen/client/metricsapi"
)

type MetricsService struct {
	gen metricsapi.ClientWithResponsesInterface
}

func NewMetricsService(gen metricsapi.ClientWithResponsesInterface) *MetricsService {
	return &MetricsService{gen: gen}
}

func (s *MetricsService) Summary(ctx context.Context, opts MetricsSummaryOptions) (metricsapi.FileUsageSummary, error) {
	params := &metricsapi.GetMetricsSummaryParams{}
	if opts.InactiveDays > 0 {
		params.InactiveDays = &opts.InactiveDays
	}
	resp, err := s.gen.GetMetricsSummaryWithResponse(ctx, params)
	if err != nil {
		return metricsapi.FileUsageSummary{}, err
	}
	if resp.JSON200 == nil {
		return metricsapi.FileUsageSummary{}, fmt.Errorf("failed to get metrics summary: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}

func (s *MetricsService) Files(ctx context.Context, opts MetricsFilesOptions) ([]metricsapi.FileUsage, error) {
	params := &metricsapi.ListMetricsFilesParams{}
	if opts.Limit > 0 {
		params.Limit = &opts.Limit
	}
	if opts.Offset > 0 {
		params.Offset = &opts.Offset
	}
	if opts.InactiveDays > 0 {
		params.InactiveDays = &opts.InactiveDays
	}

	resp, err := s.gen.ListMetricsFilesWithResponse(ctx, params)
	if err != nil {
		return nil, err
	}
	if resp.JSON200 == nil {
		return nil, fmt.Errorf("failed to list metrics files: %d", resp.StatusCode())
	}
	if resp.JSON200.Data == nil {
		return []metricsapi.FileUsage{}, nil
	}
	return *resp.JSON200.Data, nil
}

func (s *MetricsService) File(ctx context.Context, objectID string) (metricsapi.FileUsage, error) {
	resp, err := s.gen.GetMetricsFileWithResponse(ctx, objectID)
	if err != nil {
		return metricsapi.FileUsage{}, err
	}
	if resp.JSON200 == nil {
		return metricsapi.FileUsage{}, fmt.Errorf("failed to get file metrics: %d", resp.StatusCode())
	}
	return *resp.JSON200, nil
}
