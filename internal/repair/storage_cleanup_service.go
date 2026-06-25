package repair

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	drsapi "github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/models"
	"github.com/google/uuid"
)

type storageCleanupManager interface {
	ListStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error)
	ListDuplicateStorageCleanupRecords(ctx context.Context, organization, project, pathPrefix string) ([]models.StorageCleanupRecord, error)
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
	Path     string
	Records  []StorageCleanupRecordAudit
	Analysis cleanupStructuralAnalysis
}

type cleanupURLTemplate string

const (
	cleanupURLTemplateUnknown      cleanupURLTemplate = "unknown"
	cleanupURLTemplateLegacyDID    cleanupURLTemplate = "legacy_did_keyed"
	cleanupURLTemplateProjectScope cleanupURLTemplate = "project_scoped"
	cleanupURLTemplateChecksumRoot cleanupURLTemplate = "checksum_root"
	cleanupURLTemplateOther        cleanupURLTemplate = "other"
)

type cleanupStructuralAnalysis struct {
	ChecksumCount             int
	SizeCount                 int
	StructuralReason          string
	LegacyURLTemplateDetected bool
	StructurallyEquivalent    bool
	MixedContentCollision     bool
}

const defaultStorageCleanupProbeConcurrency = 8

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
	selectedPaths, hasSelectedPaths, err := normalizeExpectedPaths(req.SelectedPaths)
	if err != nil {
		return StorageCleanupReport{}, err
	}
	rows, err := s.loadCleanupRows(ctx, report.Organization, report.Project, report.PathPrefix, hasExpected)
	if err != nil {
		return StorageCleanupReport{}, err
	}
	if hasSelectedPaths {
		filtered := make([]models.StorageCleanupRecord, 0, len(rows))
		for _, row := range rows {
			if selectedPaths[row.NormalizedPath] {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
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
		record.StorageStatus = StorageProbeStatusUnknown
		state.Records = append(state.Records, record)
	}

	if checkStorage {
		if err := s.populateProbeStates(ctx, report.Organization, report.Project, grouped); err != nil {
			return StorageCleanupReport{}, err
		}
	}

	paths := make([]string, 0, len(grouped))
	for path := range grouped {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	report.ScannedPaths = len(paths)

	for _, path := range paths {
		state := grouped[path]
		sort.Slice(state.Records, func(i, j int) bool {
			return state.Records[i].ObjectID < state.Records[j].ObjectID
		})
		state.Analysis = analyzeCleanupStructuralDuplicates(state, objectByID)
		findings := classifyStorageCleanupPath(state, hasExpected, expected[state.Path])
		if len(findings) == 0 && len(state.Records) > 1 {
			findings = append(findings, newDuplicateStorageProbeErrorFinding(state))
		}
		if len(findings) == 0 {
			report.UnclassifiedPaths++
			continue
		}
		report.ClassifiedPaths++
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

func (s *StorageCleanupService) loadCleanupRows(ctx context.Context, organization, project, pathPrefix string, hasExpected bool) ([]models.StorageCleanupRecord, error) {
	if hasExpected {
		return s.om.ListStorageCleanupRecords(ctx, organization, project, pathPrefix)
	}
	return s.om.ListDuplicateStorageCleanupRecords(ctx, organization, project, pathPrefix)
}

func (s *StorageCleanupService) Apply(ctx context.Context, req StorageCleanupApplyRequest) (StorageCleanupApplyResult, error) {
	checkStorage := req.CheckStorage
	report, err := s.Audit(ctx, StorageCleanupAuditRequest{
		Organization:  req.Organization,
		Project:       req.Project,
		PathPrefix:    req.PathPrefix,
		ExpectedPaths: req.ExpectedPaths,
		SelectedPaths: req.SelectedPaths,
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

type cleanupProbeResult struct {
	Status          StorageProbeStatus
	Message         string
	CleanupScope    CleanupScope
	HasBrokenAccess bool
	Probes          []StorageCleanupAccessProbe
}

type storageCleanupProbeTask struct {
	recordIdx int
	urlIdx    int
	rawURL    string
}

type storageCleanupProbeResponse struct {
	recordIdx int
	urlIdx    int
	probe     StorageCleanupAccessProbe
}

type storageCleanupRecordSlot struct {
	state *storageCleanupPathState
	index int
}

type storageCleanupAuditContext struct {
	om           storageCleanupManager
	organization string
	project      string

	mu             sync.Mutex
	urlCache       map[string]StorageCleanupAccessProbe
	bucketErrCache map[string]StorageCleanupAccessProbe
	bucketInFlight map[string]chan struct{}
}

func (s *StorageCleanupService) populateProbeStates(ctx context.Context, organization, project string, grouped map[string]*storageCleanupPathState) error {
	ctx = core.WithStorageInspectCache(ctx)
	recordSlots := make([]storageCleanupRecordSlot, 0)
	tasks := make([]storageCleanupProbeTask, 0)
	for _, path := range sortedCleanupPaths(grouped) {
		state := grouped[path]
		for i := range state.Records {
			recordIdx := len(recordSlots)
			recordSlots = append(recordSlots, storageCleanupRecordSlot{state: state, index: i})
			urls := state.Records[i].CurrentAccessURLs
			for urlIdx, raw := range urls {
				raw = strings.TrimSpace(raw)
				if raw == "" {
					continue
				}
				tasks = append(tasks, storageCleanupProbeTask{recordIdx: recordIdx, urlIdx: urlIdx, rawURL: raw})
			}
		}
	}
	if len(recordSlots) == 0 {
		return nil
	}
	accumulators := make([][]StorageCleanupAccessProbe, len(recordSlots))
	for i, slot := range recordSlots {
		accumulators[i] = make([]StorageCleanupAccessProbe, len(slot.state.Records[slot.index].CurrentAccessURLs))
	}
	auditCtx := &storageCleanupAuditContext{
		om:             s.om,
		organization:   organization,
		project:        project,
		urlCache:       map[string]StorageCleanupAccessProbe{},
		bucketErrCache: map[string]StorageCleanupAccessProbe{},
		bucketInFlight: map[string]chan struct{}{},
	}
	if err := auditCtx.runProbes(ctx, tasks, accumulators); err != nil {
		return err
	}
	for i, slot := range recordSlots {
		record := &slot.state.Records[slot.index]
		probeState := deriveCleanupProbeResult(accumulators[i])
		record.AccessProbes = probeState.Probes
		record.StorageStatus = probeState.Status
		record.StorageMessage = probeState.Message
		record.CleanupScope = probeState.CleanupScope
	}
	return nil
}

func (a *storageCleanupAuditContext) runProbes(ctx context.Context, tasks []storageCleanupProbeTask, accumulators [][]StorageCleanupAccessProbe) error {
	if len(tasks) == 0 {
		return nil
	}
	workerCount := defaultStorageCleanupProbeConcurrency
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}
	jobs := make(chan storageCleanupProbeTask)
	results := make(chan storageCleanupProbeResponse, len(tasks))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					select {
					case errCh <- ctx.Err():
					default:
					}
					return
				case task, ok := <-jobs:
					if !ok {
						return
					}
					probe, err := a.probeURL(ctx, task.rawURL)
					if err != nil {
						select {
						case errCh <- err:
						default:
						}
						return
					}
					select {
					case <-ctx.Done():
						select {
						case errCh <- ctx.Err():
						default:
						}
						return
					case results <- storageCleanupProbeResponse{recordIdx: task.recordIdx, urlIdx: task.urlIdx, probe: probe}:
					}
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, task := range tasks {
			select {
			case <-ctx.Done():
				return
			case jobs <- task:
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	received := 0
	for received < len(tasks) {
		select {
		case err := <-errCh:
			if err != nil {
				<-done
				return err
			}
		case <-ctx.Done():
			<-done
			return ctx.Err()
		case res := <-results:
			accumulators[res.recordIdx][res.urlIdx] = res.probe
			received++
		}
	}
	<-done
	return nil
}

func (a *storageCleanupAuditContext) probeURL(ctx context.Context, raw string) (StorageCleanupAccessProbe, error) {
	raw = strings.TrimSpace(raw)
	probe := StorageCleanupAccessProbe{URL: raw}
	if raw == "" {
		return probe, nil
	}
	if bucket, _, ok := common.ParseS3URL(raw); ok {
		probe.Bucket = bucket
	}

	var (
		bucketLeader bool
		releaseWait  func()
	)
	for {
		a.mu.Lock()
		if cached, ok := a.urlCache[raw]; ok {
			a.mu.Unlock()
			return cached, nil
		}
		if probe.Bucket != "" {
			if cached, ok := a.bucketErrCache[probe.Bucket]; ok {
				cached.URL = raw
				cached.Bucket = probe.Bucket
				a.urlCache[raw] = cached
				a.mu.Unlock()
				return cached, nil
			}
			if !bucketLeader {
				if waiter, ok := a.bucketInFlight[probe.Bucket]; ok {
					a.mu.Unlock()
					select {
					case <-ctx.Done():
						return StorageCleanupAccessProbe{}, ctx.Err()
					case <-waiter:
						continue
					}
				}
				waiter := make(chan struct{})
				a.bucketInFlight[probe.Bucket] = waiter
				bucketLeader = true
				releaseWait = func() {
					a.mu.Lock()
					delete(a.bucketInFlight, probe.Bucket)
					close(waiter)
					a.mu.Unlock()
				}
			}
		}
		a.mu.Unlock()
		break
	}
	if releaseWait != nil {
		defer releaseWait()
	}

	if err := ctx.Err(); err != nil {
		return StorageCleanupAccessProbe{}, err
	}

	_, err := a.om.InspectStorageObject(ctx, core.InspectStorageRequest{
		Organization: a.organization,
		Project:      a.project,
		ObjectURL:    raw,
	})
	if err == nil {
		probe.StorageStatus = StorageProbeStatusLive
		a.cacheProbe(raw, probe)
		return probe, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return StorageCleanupAccessProbe{}, err
	}
	if inspectErr, ok := err.(*core.StorageInspectError); ok {
		probe.ErrorKind = string(inspectErr.Kind)
		probe.StorageMessage = inspectErr.Error()
		switch inspectErr.Kind {
		case core.StorageInspectObjectNotFound:
			probe.StorageStatus = StorageProbeStatusMissing
		default:
			probe.StorageStatus = StorageProbeStatusError
			if probe.Bucket != "" && cacheableBucketInspectKind(inspectErr.Kind) {
				a.cacheBucketError(probe.Bucket, probe)
			}
		}
		a.cacheProbe(raw, probe)
		return probe, nil
	}
	probe.StorageStatus = StorageProbeStatusError
	probe.StorageMessage = err.Error()
	a.cacheProbe(raw, probe)
	return probe, nil
}

func (a *storageCleanupAuditContext) cacheProbe(raw string, probe StorageCleanupAccessProbe) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.urlCache[raw] = probe
}

func (a *storageCleanupAuditContext) cacheBucketError(bucket string, probe StorageCleanupAccessProbe) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.bucketErrCache[bucket] = probe
}

func cacheableBucketInspectKind(kind core.StorageInspectErrorKind) bool {
	switch kind {
	case core.StorageInspectCredentialMissing, core.StorageInspectPermissionDenied, core.StorageInspectUnsupported:
		return true
	default:
		return false
	}
}

func deriveCleanupProbeResult(probes []StorageCleanupAccessProbe) cleanupProbeResult {
	if len(probes) == 0 {
		return cleanupProbeResult{
			Status:       StorageProbeStatusUnknown,
			Message:      "record has no access URLs",
			CleanupScope: CleanupScopeRecord,
		}
	}

	var (
		liveCount       int
		missingCount    int
		errorCount      int
		usableProbeSeen bool
		lastMessage     string
		filtered        = make([]StorageCleanupAccessProbe, 0, len(probes))
	)
	for _, probe := range probes {
		if strings.TrimSpace(probe.URL) == "" {
			continue
		}
		filtered = append(filtered, probe)
		switch probe.StorageStatus {
		case StorageProbeStatusLive:
			liveCount++
			usableProbeSeen = true
		case StorageProbeStatusMissing:
			missingCount++
			usableProbeSeen = true
			lastMessage = probe.StorageMessage
		case StorageProbeStatusError:
			errorCount++
			usableProbeSeen = true
			lastMessage = probe.StorageMessage
		}
	}
	if len(filtered) == 0 {
		return cleanupProbeResult{
			Status:       StorageProbeStatusUnknown,
			Message:      "record has no usable access URLs",
			CleanupScope: CleanupScopeRecord,
		}
	}

	result := cleanupProbeResult{Probes: filtered}
	switch {
	case liveCount > 0:
		result.Status = StorageProbeStatusLive
		if errorCount > 0 {
			result.CleanupScope = CleanupScopeAccessURL
			result.Message = "record has one or more broken access URLs"
			result.HasBrokenAccess = true
		}
	case missingCount > 0 && errorCount == 0:
		result.Status = StorageProbeStatusMissing
		result.CleanupScope = CleanupScopeRecord
		result.Message = "storage object not found"
	case errorCount > 0:
		result.Status = StorageProbeStatusError
		result.CleanupScope = CleanupScopeRecord
		result.Message = lastMessage
		result.HasBrokenAccess = true
	case usableProbeSeen:
		result.Status = StorageProbeStatusUnknown
		result.CleanupScope = CleanupScopeRecord
		result.Message = "record could not be classified from storage probes"
	default:
		result.Status = StorageProbeStatusUnknown
		result.CleanupScope = CleanupScopeRecord
		result.Message = "record has no usable access URLs"
	}
	return result
}

func classifyStorageCleanupPath(state *storageCleanupPathState, hasExpected, inExpectedSet bool) []StorageCleanupFinding {
	liveCount, missingCount, errorCount, unknownCount := 0, 0, 0, 0
	hasBrokenAccess := false
	for _, rec := range state.Records {
		switch rec.StorageStatus {
		case StorageProbeStatusLive:
			liveCount++
		case StorageProbeStatusMissing:
			missingCount++
		case StorageProbeStatusError:
			errorCount++
		case StorageProbeStatusUnknown:
			unknownCount++
		}
		if recordHasBrokenAccess(rec) {
			hasBrokenAccess = true
		}
	}
	findings := make([]StorageCleanupFinding, 0, 3)
	if len(state.Records) > 1 {
		switch {
		case liveCount > 1:
			findings = append(findings, withStructuralContext(StorageCleanupFinding{
				Kind:              FindingLiveDuplicateConflict,
				Severity:          SeverityWarn,
				NormalizedPath:    state.Path,
				Message:           fmt.Sprintf("multiple records for %s still resolve in storage", state.Path),
				RecommendedAction: "manual_review",
				Records:           cloneCleanupRecords(state.Records),
			}, state.Analysis))
		case liveCount >= 1 && missingCount >= 1:
			findings = append(findings, withStructuralContext(StorageCleanupFinding{
				Kind:              FindingStaleDuplicateRecord,
				Severity:          SeverityWarn,
				NormalizedPath:    state.Path,
				Message:           fmt.Sprintf("duplicate path %s has stale records that no longer resolve", state.Path),
				RecommendedAction: "delete_stale_duplicates",
				Records:           cloneCleanupRecords(state.Records),
			}, state.Analysis))
		case errorCount > 0 && !hasBrokenAccess:
			findings = append(findings, newDuplicateStorageProbeErrorFinding(state))
		case unknownCount > 0 || (errorCount > 0 && hasBrokenAccess) || (liveCount > 0 && unknownCount > 0) || (missingCount > 0 && unknownCount > 0):
			findings = append(findings, newDuplicateStorageProbeErrorFinding(state))
		}
	}
	if hasBrokenAccess {
		findings = append(findings, withStructuralContext(StorageCleanupFinding{
			Kind:              FindingBrokenAccessURLError,
			Severity:          SeverityWarn,
			NormalizedPath:    state.Path,
			Message:           fmt.Sprintf("path %s includes one or more broken access URLs", state.Path),
			RecommendedAction: "manual_review",
			CleanupScope:      cleanupScopeForFinding(state.Records),
			Records:           cloneCleanupRecords(filterCleanupRecordsWithBrokenAccess(state.Records)),
		}, state.Analysis))
	}
	if hasExpected && !inExpectedSet {
		switch {
		case liveCount > 0:
			findings = append(findings, withStructuralContext(StorageCleanupFinding{
				Kind:                FindingRepoOrphanLiveObject,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents but still resolves in storage", state.Path),
				RecommendedAction:   "delete_repo_orphans",
				RepoDeleteCandidate: true,
				Records:             cloneCleanupRecords(state.Records),
			}, state.Analysis))
		case missingCount > 0 && errorCount == 0:
			findings = append(findings, withStructuralContext(StorageCleanupFinding{
				Kind:                FindingRepoOrphanStaleRecord,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents and storage is already missing", state.Path),
				RecommendedAction:   "delete_repo_orphans",
				RepoDeleteCandidate: true,
				Records:             cloneCleanupRecords(state.Records),
			}, state.Analysis))
		case errorCount > 0:
			findings = append(findings, withStructuralContext(StorageCleanupFinding{
				Kind:                FindingStorageProbeError,
				Severity:            SeverityWarn,
				NormalizedPath:      state.Path,
				Message:             fmt.Sprintf("path %s is absent from expected repo contents but storage verification failed", state.Path),
				RecommendedAction:   "manual_review",
				RepoDeleteCandidate: true,
				CleanupScope:        cleanupScopeForFinding(state.Records),
				Records:             cloneCleanupRecords(state.Records),
			}, state.Analysis))
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
				if rec.CleanupScope == CleanupScopeAccessURL {
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
				if rec.CleanupScope == CleanupScopeAccessURL {
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

func sortedCleanupPaths(grouped map[string]*storageCleanupPathState) []string {
	paths := make([]string, 0, len(grouped))
	for path := range grouped {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
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

func analyzeCleanupStructuralDuplicates(state *storageCleanupPathState, objectByID map[string]models.InternalObject) cleanupStructuralAnalysis {
	checksumSet := map[string]bool{}
	sizeSet := map[int64]bool{}
	templateSet := map[cleanupURLTemplate]bool{}
	legacyDetected := false
	for _, record := range state.Records {
		sizeSet[record.Size] = true
		if obj, ok := objectByID[record.ObjectID]; ok {
			if checksum, ok := common.CanonicalSHA256(obj.Checksums); ok && checksum != "" {
				checksumSet[strings.ToLower(strings.TrimSpace(checksum))] = true
				for _, rawURL := range record.CurrentAccessURLs {
					tpl := detectCleanupURLTemplate(rawURL, checksum)
					templateSet[tpl] = true
					if tpl == cleanupURLTemplateLegacyDID {
						legacyDetected = true
					}
				}
				continue
			}
		}
		for _, rawURL := range record.CurrentAccessURLs {
			tpl := detectCleanupURLTemplate(rawURL, "")
			templateSet[tpl] = true
			if tpl == cleanupURLTemplateLegacyDID {
				legacyDetected = true
			}
		}
	}
	analysis := cleanupStructuralAnalysis{
		ChecksumCount:             len(checksumSet),
		SizeCount:                 len(sizeSet),
		LegacyURLTemplateDetected: legacyDetected,
	}
	sameChecksum := len(checksumSet) == 1 && len(state.Records) > 1
	sameSize := len(sizeSet) == 1 && len(state.Records) > 1
	multipleTemplates := len(templateSet) > 1
	switch {
	case sameChecksum && sameSize && legacyDetected && multipleTemplates:
		analysis.StructurallyEquivalent = true
		analysis.StructuralReason = "same_checksum_same_size_legacy_url_mismatch"
	case sameChecksum && sameSize && multipleTemplates:
		analysis.StructurallyEquivalent = true
		analysis.StructuralReason = "same_checksum_same_size_url_template_mismatch"
	case sameChecksum && sameSize:
		analysis.StructurallyEquivalent = true
		analysis.StructuralReason = "same_checksum_same_size"
	case len(checksumSet) > 1 || len(sizeSet) > 1:
		analysis.MixedContentCollision = true
		analysis.StructuralReason = "mixed_checksum_or_size_duplicates"
	case len(checksumSet) == 0:
		analysis.StructuralReason = "duplicate_path_no_checksum_evidence"
	default:
		analysis.StructuralReason = "duplicate_path_structural_mismatch"
	}
	return analysis
}

func detectCleanupURLTemplate(rawURL string, checksum string) cleanupURLTemplate {
	_, key, ok := common.ParseS3URL(strings.TrimSpace(rawURL))
	if !ok {
		return cleanupURLTemplateUnknown
	}
	segments := make([]string, 0)
	for _, segment := range strings.Split(strings.Trim(key, "/"), "/") {
		segment = strings.TrimSpace(segment)
		if segment != "" {
			segments = append(segments, segment)
		}
	}
	if len(segments) == 0 {
		return cleanupURLTemplateUnknown
	}
	last := strings.TrimSpace(segments[len(segments)-1])
	if checksum != "" && !strings.EqualFold(last, checksum) {
		return cleanupURLTemplateOther
	}
	if len(segments) >= 2 {
		if _, err := uuid.Parse(strings.TrimSpace(segments[0])); err == nil {
			return cleanupURLTemplateLegacyDID
		}
		return cleanupURLTemplateProjectScope
	}
	if checksum != "" && strings.EqualFold(segments[0], checksum) {
		return cleanupURLTemplateChecksumRoot
	}
	return cleanupURLTemplateOther
}

func withStructuralContext(finding StorageCleanupFinding, analysis cleanupStructuralAnalysis) StorageCleanupFinding {
	finding.StructuralReason = analysis.StructuralReason
	finding.ChecksumCount = analysis.ChecksumCount
	finding.SizeCount = analysis.SizeCount
	finding.LegacyURLTemplateDetected = analysis.LegacyURLTemplateDetected
	return finding
}

func newDuplicateStorageProbeErrorFinding(state *storageCleanupPathState) StorageCleanupFinding {
	return withStructuralContext(StorageCleanupFinding{
		Kind:              FindingStorageProbeError,
		Severity:          SeverityWarn,
		NormalizedPath:    state.Path,
		Message:           fmt.Sprintf("duplicate path %s could not be fully classified after storage verification", state.Path),
		RecommendedAction: "manual_review",
		CleanupScope:      cleanupScopeForFinding(state.Records),
		Records:           cloneCleanupRecords(state.Records),
	}, state.Analysis)
}

func cloneCleanupRecords(in []StorageCleanupRecordAudit) []StorageCleanupRecordAudit {
	out := make([]StorageCleanupRecordAudit, len(in))
	copy(out, in)
	return out
}

func filterCleanupRecordsWithBrokenAccess(in []StorageCleanupRecordAudit) []StorageCleanupRecordAudit {
	out := make([]StorageCleanupRecordAudit, 0, len(in))
	for _, record := range in {
		if recordHasBrokenAccess(record) {
			out = append(out, record)
		}
	}
	return out
}

func recordHasBrokenAccess(record StorageCleanupRecordAudit) bool {
	for _, probe := range record.AccessProbes {
		if probe.StorageStatus == StorageProbeStatusError {
			return true
		}
	}
	return false
}

func cleanupScopeForFinding(records []StorageCleanupRecordAudit) CleanupScope {
	for _, record := range records {
		if record.CleanupScope == CleanupScopeAccessURL {
			return CleanupScopeAccessURL
		}
	}
	return CleanupScopeRecord
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
