package repair

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/calypr/syfon/internal/models"
)

type projectDiffManager interface {
	ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error)
	ListDuplicateStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error)
}

type ProjectDiffService struct {
	om projectDiffManager
}

func NewProjectDiffService(om projectDiffManager) *ProjectDiffService {
	return &ProjectDiffService{om: om}
}

type projectDiffAggregate struct {
	path             string
	objectIDs        map[string]bool
	recordCount      int
	sizeBytes        int64
	downloadCount    int64
	lastDownloadTime *time.Time
}

func (s *ProjectDiffService) Audit(ctx context.Context, req ProjectDiffAuditRequest) (ProjectDiffReport, error) {
	report := ProjectDiffReport{
		Organization: strings.TrimSpace(req.Organization),
		Project:      strings.TrimSpace(req.Project),
		PathPrefix:   strings.TrimSpace(req.PathPrefix),
		Summary: ProjectDiffSummary{
			CountsByKind: map[FindingKind]int{},
		},
	}

	expected, hasExpected, err := normalizeExpectedPaths(req.ExpectedPaths)
	if err != nil {
		return ProjectDiffReport{}, err
	}

	rows, err := s.loadRows(ctx, report.Organization, report.Project, report.PathPrefix, hasExpected)
	if err != nil {
		return ProjectDiffReport{}, err
	}

	grouped := make(map[string]*projectDiffAggregate)
	for _, row := range rows {
		state := grouped[row.NormalizedPath]
		if state == nil {
			state = &projectDiffAggregate{
				path:      row.NormalizedPath,
				objectIDs: map[string]bool{},
			}
			grouped[row.NormalizedPath] = state
		}
		state.recordCount++
		state.sizeBytes += row.Size
		state.downloadCount += row.DownloadCount
		state.objectIDs[row.ObjectID] = true
		if row.LastDownloadTime != nil {
			candidate := row.LastDownloadTime.UTC()
			if state.lastDownloadTime == nil || candidate.After(*state.lastDownloadTime) {
				state.lastDownloadTime = &candidate
			}
		}
	}

	report.Summary.ScannedRecordCount = len(rows)
	report.Summary.IndexedPathCount = len(grouped)
	report.Summary.ExpectedPathCount = len(expected)
	report.Summary.IncludesRepoManifest = hasExpected

	paths := make([]string, 0, len(grouped))
	for path := range grouped {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		agg := grouped[path]
		if agg == nil {
			continue
		}
		if hasExpected && expected[path] {
			report.Summary.MatchedPathCount++
		}
		if agg.recordCount > 1 {
			report.Findings = append(report.Findings, buildProjectDiffFinding(
				FindingDuplicateSyfonPaths,
				SeverityWarn,
				agg,
				"Review duplicate records before deleting anything.",
			))
			report.Summary.CountsByKind[FindingDuplicateSyfonPaths]++
		}
		if hasExpected && !expected[path] {
			report.Findings = append(report.Findings, buildProjectDiffFinding(
				FindingSyfonMissingInRepo,
				SeverityWarn,
				agg,
				"Prepare delete to verify storage before removing Syfon-only records.",
			))
			report.Summary.CountsByKind[FindingSyfonMissingInRepo]++
		}
	}

	if hasExpected {
		missing := make([]string, 0)
		for path := range expected {
			if _, ok := grouped[path]; ok {
				continue
			}
			missing = append(missing, path)
		}
		sort.Strings(missing)
		for _, path := range missing {
			report.Findings = append(report.Findings, ProjectDiffFinding{
				Kind:              FindingRepoMissingInSyfon,
				Severity:          SeverityInfo,
				NormalizedPath:    path,
				RecommendedAction: "Review missing Syfon records for this Git-tracked path.",
			})
			report.Summary.CountsByKind[FindingRepoMissingInSyfon]++
		}
	}

	report.Summary.TotalFindings = len(report.Findings)
	if len(report.Summary.CountsByKind) == 0 {
		report.Summary.CountsByKind = nil
	}
	return report, nil
}

func (s *ProjectDiffService) loadRows(ctx context.Context, organization, project, pathPrefix string, hasExpected bool) ([]models.StorageCleanupRecord, error) {
	if hasExpected {
		return s.om.ListStorageCleanupRecords(ctx, organization, project, pathPrefix)
	}
	return s.om.ListDuplicateStorageCleanupRecords(ctx, organization, project, pathPrefix)
}

func buildProjectDiffFinding(kind FindingKind, severity Severity, agg *projectDiffAggregate, recommendedAction string) ProjectDiffFinding {
	objectIDs := make([]string, 0, len(agg.objectIDs))
	for objectID := range agg.objectIDs {
		objectIDs = append(objectIDs, objectID)
	}
	sort.Strings(objectIDs)
	return ProjectDiffFinding{
		Kind:              kind,
		Severity:          severity,
		NormalizedPath:    agg.path,
		ObjectIDs:         objectIDs,
		RecordCount:       agg.recordCount,
		SizeBytes:         agg.sizeBytes,
		DownloadCount:     agg.downloadCount,
		LastDownloadTime:  agg.lastDownloadTime,
		RecommendedAction: recommendedAction,
	}
}
