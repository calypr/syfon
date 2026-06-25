package services

import (
	"context"
	"net/http"

	"github.com/calypr/syfon/client/request"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/repair"
)

type RepairService struct {
	requestor request.Requester
}

func NewRepairService(r request.Requester) *RepairService {
	return &RepairService{requestor: r}
}

func (s *RepairService) ProjectDiffAudit(ctx context.Context, opts ProjectDiffAuditOptions) (repair.ProjectDiffReport, error) {
	req := repair.ProjectDiffAuditRequest{
		Organization:  opts.Organization,
		Project:       opts.ProjectID,
		PathPrefix:    opts.PathPrefix,
		ExpectedPaths: append([]string(nil), opts.ExpectedPaths...),
	}
	var out repair.ProjectDiffReport
	if err := s.requestor.Do(ctx, http.MethodPost, common.RouteInternalRepairProjectDiff, req, &out); err != nil {
		return repair.ProjectDiffReport{}, err
	}
	return out, nil
}

func (s *RepairService) StorageCleanupAudit(ctx context.Context, opts StorageCleanupAuditOptions) (repair.StorageCleanupReport, error) {
	req := repair.StorageCleanupAuditRequest{
		Organization:  opts.Organization,
		Project:       opts.ProjectID,
		PathPrefix:    opts.PathPrefix,
		ExpectedPaths: append([]string(nil), opts.ExpectedPaths...),
		SelectedPaths: append([]string(nil), opts.SelectedPaths...),
		CheckStorage:  opts.CheckStorage,
	}
	var out repair.StorageCleanupReport
	if err := s.requestor.Do(ctx, http.MethodPost, common.RouteInternalRepairCleanupAudit, req, &out); err != nil {
		return repair.StorageCleanupReport{}, err
	}
	return out, nil
}

func (s *RepairService) StorageCleanupApply(ctx context.Context, opts StorageCleanupApplyOptions) (repair.StorageCleanupApplyResult, error) {
	kinds := make([]repair.FindingKind, 0, len(opts.SelectedFindingKinds))
	for _, kind := range opts.SelectedFindingKinds {
		kinds = append(kinds, repair.FindingKind(kind))
	}
	req := repair.StorageCleanupApplyRequest{
		Organization:          opts.Organization,
		Project:               opts.ProjectID,
		PathPrefix:            opts.PathPrefix,
		ExpectedPaths:         append([]string(nil), opts.ExpectedPaths...),
		DeleteStaleDuplicates: opts.DeleteStaleDuplicates,
		DeleteRepoOrphans:     opts.DeleteRepoOrphans,
		DryRun:                opts.DryRun,
		SelectedPaths:         append([]string(nil), opts.SelectedPaths...),
		SelectedObjectIDs:     append([]string(nil), opts.SelectedObjectIDs...),
		SelectedFindingKinds:  kinds,
		CheckStorage:          opts.CheckStorage,
	}
	var out repair.StorageCleanupApplyResult
	if err := s.requestor.Do(ctx, http.MethodPost, common.RouteInternalRepairCleanupApply, req, &out); err != nil {
		return repair.StorageCleanupApplyResult{}, err
	}
	return out, nil
}
