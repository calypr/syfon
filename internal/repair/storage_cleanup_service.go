package repair

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	drsapi "github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
)

type storageCleanupManager interface {
	ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error)
	GetBulkObjects(ctx context.Context, ids []string, requiredMethod string) ([]models.InternalObject, error)
	InspectStorageObject(ctx context.Context, req core.InspectStorageRequest) (*core.StorageObjectMetadata, error)
	DeleteObjectWithOptions(ctx context.Context, id string, opts core.DeleteOptions) error
}

type StorageCleanupService struct {
	om storageCleanupManager
}

func NewStorageCleanupService(om storageCleanupManager) *StorageCleanupService {
	return &StorageCleanupService{om: om}
}

type storageCleanupPathState struct {
	Path    string
	Records []StorageCleanupRecordAudit
}

func (s *StorageCleanupService) Audit(ctx context.Context, req StorageCleanupAuditRequest) (StorageCleanupReport, error) {
	checkStorage := true
	if req.CheckStorage != nil {
		checkStorage = *req.CheckStorage
	}
	report := StorageCleanupReport{
		Organization: strings.TrimSpace(req.Organization),
		Project:      strings.TrimSpace(req.Project),
		PathPrefix:   strings.TrimSpace(req.PathPrefix),
		Summary:      map[FindingKind]int{},
	}
	expected, hasExpected, err := normalizeExpectedPaths(req.ExpectedPaths)
	if err != nil {
		return StorageCleanupReport{}, err
	}
	rows, err := s.om.ListStorageCleanupRecords(ctx, report.Organization, report.Project, report.PathPrefix)
	if err != nil {
		return StorageCleanupReport{}, err
	}
	report.Scanned = len(rows)

	ids := uniqueCleanupIDs(rows)
	objects, err := s.om.GetBulkObjects(ctx, ids, "read")
	if err != nil {
		return StorageCleanupReport{}, err
	}
	objectByID := make(map[string]models.InternalObject, len(objects))
	for _, obj := range objects {
		objectByID[strings.TrimSpace(obj.Id)] = obj
	}

	grouped := make(map[string]*storageCleanupPathState)
	for _, row := range rows {
		state := grouped[row.NormalizedPath]
		if state == nil {
			state = &storageCleanupPathState{Path: row.NormalizedPath}
			grouped[row.NormalizedPath] = state
		}
		record := StorageCleanupRecordAudit{
			ObjectID:         row.ObjectID,
			NormalizedPath:   row.NormalizedPath,
			Size:             row.Size,
			DownloadCount:    row.DownloadCount,
			LastDownloadTime: utcTimePtr(row.LastDownloadTime),
			UpdatedTime:      utcTimeValue(row.UpdatedTime),
		}
		if obj, ok := objectByID[row.ObjectID]; ok && obj.AccessMethods != nil {
			record.CurrentAccessURLs = storageCleanupAccessMethodURLs(*obj.AccessMethods)
		}
		if checkStorage {
			record.StorageStatus, record.StorageMessage = s.inspectCleanupRecord(ctx, report.Organization, report.Project, record.CurrentAccessURLs)
		} else {
			record.StorageStatus = StorageProbeStatusUnknown
		}
		state.Records = append(state.Records, record)
	}

	paths := make([]string, 0, len(grouped))
	for path := range grouped {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		state := grouped[path]
		sort.Slice(state.Records, func(i, j int) bool {
			return state.Records[i].ObjectID < state.Records[j].ObjectID
		})
		findings := classifyStorageCleanupPath(state, hasExpected, expected[state.Path])
		for _, finding := range findings {
			report.Summary[finding.Kind]++
			report.Findings = append(report.Findings, finding)
		}
	}
	if len(report.Summary) == 0 {
		report.Summary = nil
	}
	return report, nil
}

func (s *StorageCleanupService) Apply(ctx context.Context, req StorageCleanupApplyRequest) (StorageCleanupApplyResult, error) {
	checkStorage := req.CheckStorage
	report, err := s.Audit(ctx, StorageCleanupAuditRequest{
		Organization:  req.Organization,
		Project:       req.Project,
		PathPrefix:    req.PathPrefix,
		ExpectedPaths: req.ExpectedPaths,
		CheckStorage:  checkStorage,
	})
	if err != nil {
		return StorageCleanupApplyResult{}, err
	}
	result := StorageCleanupApplyResult{Report: report, DryRun: req.DryRun}
	actions, repoPaths := buildStorageCleanupActions(report, req)
	result.RepoDeletePaths = repoPaths
	if req.DryRun {
		return result, nil
	}
	ids := make([]string, 0, len(actions))
	for id := range actions {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, objectID := range ids {
		action := actions[objectID]
		if err := s.om.DeleteObjectWithOptions(ctx, objectID, core.DeleteOptions{DeleteStorageData: action.DeleteStorageData}); err != nil {
			result.Skipped = append(result.Skipped, StorageCleanupSkipped{
				NormalizedPath: action.Path,
				ObjectID:       objectID,
				Kind:           action.Kind,
				Reason:         err.Error(),
			})
			if action.DeleteStorageData {
				result.StoragePurgeResults = append(result.StoragePurgeResults, StorageCleanupStoragePurgeResult{
					ObjectID: objectID,
					Purged:   false,
					Message:  err.Error(),
				})
			}
			continue
		}
		result.DeletedRecordIDs = append(result.DeletedRecordIDs, objectID)
		if action.DeleteStorageData {
			result.StoragePurgeResults = append(result.StoragePurgeResults, StorageCleanupStoragePurgeResult{
				ObjectID: objectID,
				Purged:   true,
			})
		}
	}
	return result, nil
}

func (s *StorageCleanupService) inspectCleanupRecord(ctx context.Context, organization, project string, urls []string) (StorageProbeStatus, string) {
	if len(urls) == 0 {
		return StorageProbeStatusUnknown, "record has no access URLs"
	}
	var sawMissing bool
	var lastErr error
	for _, raw := range urls {
		if strings.TrimSpace(raw) == "" {
			continue
		}
		_, err := s.om.InspectStorageObject(ctx, core.InspectStorageRequest{
			Organization: organization,
			Project:      project,
			ObjectURL:    raw,
		})
		if err == nil {
			return StorageProbeStatusLive, ""
		}
		if inspectErr, ok := err.(*core.StorageInspectError); ok && inspectErr.Kind == core.StorageInspectObjectNotFound {
			sawMissing = true
			lastErr = err
			continue
		}
		lastErr = err
	}
	if sawMissing {
		return StorageProbeStatusMissing, "storage object not found"
	}
	if lastErr != nil {
		return StorageProbeStatusError, lastErr.Error()
	}
	return StorageProbeStatusUnknown, ""
}

func classifyStorageCleanupPath(state *storageCleanupPathState, hasExpected, inExpectedSet bool) []StorageCleanupFinding {
	liveCount, missingCount, errorCount := 0, 0, 0
	for _, rec := range state.Records {
		switch rec.StorageStatus {
		case StorageProbeStatusLive:
			liveCount++
		case StorageProbeStatusMissing:
			missingCount++
		case StorageProbeStatusError:
			errorCount++
		}
	}
	findings := make([]StorageCleanupFinding, 0, 2)
	if len(state.Records) > 1 {
		switch {
		case liveCount > 1:
			findings = append(findings, StorageCleanupFinding{
				Kind:              FindingLiveDuplicateConflict,
				Severity:          SeverityWarn,
				NormalizedPath:    state.Path,
				Message:           fmt.Sprintf("multiple records for %s still resolve in storage", state.Path),
				RecommendedAction: "manual_review",
				Records:           cloneCleanupRecords(state.Records),
			})
		case liveCount >= 1 && missingCount >= 1 && errorCount == 0:
			findings = append(findings, StorageCleanupFinding{
				Kind:              FindingStaleDuplicateRecord,
				Severity:          SeverityWarn,
				NormalizedPath:    state.Path,
				Message:           fmt.Sprintf("duplicate path %s has stale records that no longer resolve", state.Path),
				RecommendedAction: "delete_stale_duplicates",
				Records:           cloneCleanupRecords(state.Records),
			})
		case errorCount > 0:
			findings = append(findings, StorageCleanupFinding{
				Kind:              FindingStorageProbeError,
				Severity:          SeverityWarn,
				NormalizedPath:    state.Path,
				Message:           fmt.Sprintf("duplicate path %s could not be fully verified in storage", state.Path),
				RecommendedAction: "manual_review",
				Records:           cloneCleanupRecords(state.Records),
			})
		}
	}
	if hasExpected && !inExpectedSet {
		switch {
		case liveCount > 0:
			findings = append(findings, StorageCleanupFinding{
				Kind:                FindingRepoOrphanLiveObject,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents but still resolves in storage", state.Path),
				RecommendedAction:   "delete_repo_orphans",
				RepoDeleteCandidate: true,
				Records:             cloneCleanupRecords(state.Records),
			})
		case missingCount > 0 && errorCount == 0:
			findings = append(findings, StorageCleanupFinding{
				Kind:                FindingRepoOrphanStaleRecord,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents and storage is already missing", state.Path),
				RecommendedAction:   "delete_repo_orphans",
				RepoDeleteCandidate: true,
				Records:             cloneCleanupRecords(state.Records),
			})
		case errorCount > 0:
			findings = append(findings, StorageCleanupFinding{
				Kind:                FindingStorageProbeError,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents but storage verification failed", state.Path),
				RecommendedAction:   "manual_review",
				RepoDeleteCandidate: true,
				Records:             cloneCleanupRecords(state.Records),
			})
		}
	}
	return findings
}

type storageCleanupAction struct {
	Path              string
	Kind              FindingKind
	DeleteStorageData bool
}

func buildStorageCleanupActions(report StorageCleanupReport, req StorageCleanupApplyRequest) (map[string]storageCleanupAction, []string) {
	pathFilter := make(map[string]bool, len(req.SelectedPaths))
	for _, path := range req.SelectedPaths {
		if normalized, _, err := common.NormalizeBrowsePath(path); err == nil {
			pathFilter[normalized] = true
		}
	}
	objectFilter := make(map[string]bool, len(req.SelectedObjectIDs))
	for _, objectID := range req.SelectedObjectIDs {
		objectID = strings.TrimSpace(objectID)
		if objectID != "" {
			objectFilter[objectID] = true
		}
	}
	kindFilter := make(map[FindingKind]bool, len(req.SelectedFindingKinds))
	for _, kind := range req.SelectedFindingKinds {
		kindFilter[kind] = true
	}
	repoPaths := make(map[string]bool)
	actions := map[string]storageCleanupAction{}
	for _, finding := range report.Findings {
		if len(pathFilter) > 0 && !pathFilter[finding.NormalizedPath] {
			continue
		}
		if len(kindFilter) > 0 && !kindFilter[finding.Kind] {
			continue
		}
		switch finding.Kind {
		case FindingStaleDuplicateRecord:
			if !req.DeleteStaleDuplicates {
				continue
			}
			for _, rec := range finding.Records {
				if rec.StorageStatus != StorageProbeStatusMissing {
					continue
				}
				if len(objectFilter) > 0 && !objectFilter[rec.ObjectID] {
					continue
				}
				actions[rec.ObjectID] = storageCleanupAction{Path: finding.NormalizedPath, Kind: finding.Kind}
			}
		case FindingRepoOrphanLiveObject, FindingRepoOrphanStaleRecord:
			if !req.DeleteRepoOrphans {
				continue
			}
			repoPaths[finding.NormalizedPath] = true
			for _, rec := range finding.Records {
				if len(objectFilter) > 0 && !objectFilter[rec.ObjectID] {
					continue
				}
				switch rec.StorageStatus {
				case StorageProbeStatusLive:
					actions[rec.ObjectID] = storageCleanupAction{Path: finding.NormalizedPath, Kind: finding.Kind, DeleteStorageData: true}
				case StorageProbeStatusMissing:
					if current, ok := actions[rec.ObjectID]; !ok || !current.DeleteStorageData {
						actions[rec.ObjectID] = storageCleanupAction{Path: finding.NormalizedPath, Kind: finding.Kind}
					}
				}
			}
		}
	}
	outPaths := make([]string, 0, len(repoPaths))
	for path := range repoPaths {
		outPaths = append(outPaths, path)
	}
	sort.Strings(outPaths)
	return actions, outPaths
}

func normalizeExpectedPaths(values []string) (map[string]bool, bool, error) {
	if len(values) == 0 {
		return nil, false, nil
	}
	out := make(map[string]bool, len(values))
	for _, raw := range values {
		normalized, _, err := common.NormalizeBrowsePath(raw)
		if err != nil {
			return nil, false, err
		}
		out[normalized] = true
	}
	return out, true, nil
}

func uniqueCleanupIDs(rows []models.StorageCleanupRecord) []string {
	seen := make(map[string]bool, len(rows))
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		if seen[row.ObjectID] {
			continue
		}
		seen[row.ObjectID] = true
		out = append(out, row.ObjectID)
	}
	sort.Strings(out)
	return out
}

func storageCleanupAccessMethodURLs(methods []drsapi.AccessMethod) []string {
	out := make([]string, 0, len(methods))
	for _, method := range methods {
		if method.AccessUrl == nil {
			continue
		}
		raw := strings.TrimSpace(method.AccessUrl.Url)
		if raw != "" {
			out = append(out, raw)
		}
	}
	return out
}

func cloneCleanupRecords(in []StorageCleanupRecordAudit) []StorageCleanupRecordAudit {
	out := make([]StorageCleanupRecordAudit, len(in))
	copy(out, in)
	return out
}

func utcTimeValue(v time.Time) *time.Time {
	if v.IsZero() {
		return nil
	}
	t := v.UTC()
	return &t
}

func utcTimePtr(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	t := v.UTC()
	return &t
}
