package repair

import "time"

type StorageProbeStatus string

const (
	StorageProbeStatusUnknown StorageProbeStatus = "unknown"
	StorageProbeStatusLive    StorageProbeStatus = "live"
	StorageProbeStatusMissing StorageProbeStatus = "missing"
	StorageProbeStatusError   StorageProbeStatus = "error"
)

const (
	FindingStaleDuplicateRecord  FindingKind = "stale_duplicate_record"
	FindingLiveDuplicateConflict FindingKind = "live_duplicate_conflict"
	FindingBrokenAccessURLError  FindingKind = "broken_access_url_error"
	FindingRepoOrphanLiveObject  FindingKind = "repo_orphan_live_object"
	FindingRepoOrphanStaleRecord FindingKind = "repo_orphan_stale_record"
)

type CleanupScope string

const (
	CleanupScopeRecord    CleanupScope = "record"
	CleanupScopeAccessURL CleanupScope = "access_url"
)

type StorageCleanupAccessProbe struct {
	URL            string             `json:"url"`
	Bucket         string             `json:"bucket,omitempty"`
	StorageStatus  StorageProbeStatus `json:"storage_status"`
	StorageMessage string             `json:"storage_message,omitempty"`
	ErrorKind      string             `json:"error_kind,omitempty"`
}

type StorageCleanupRecordAudit struct {
	ObjectID          string                      `json:"object_id"`
	NormalizedPath    string                      `json:"normalized_path"`
	Size              int64                       `json:"size"`
	UpdatedTime       *time.Time                  `json:"updated_time,omitempty"`
	DownloadCount     int64                       `json:"download_count"`
	LastDownloadTime  *time.Time                  `json:"last_download_time,omitempty"`
	CurrentAccessURLs []string                    `json:"current_access_urls,omitempty"`
	AccessProbes      []StorageCleanupAccessProbe `json:"access_probes,omitempty"`
	CleanupScope      CleanupScope                `json:"cleanup_scope,omitempty"`
	StorageStatus     StorageProbeStatus          `json:"storage_status"`
	StorageMessage    string                      `json:"storage_message,omitempty"`
}

type StorageCleanupFinding struct {
	Kind                      FindingKind                 `json:"kind"`
	Severity                  Severity                    `json:"severity"`
	NormalizedPath            string                      `json:"normalized_path"`
	Message                   string                      `json:"message"`
	RecommendedAction         string                      `json:"recommended_action,omitempty"`
	RepoDeleteCandidate       bool                        `json:"repo_delete_candidate"`
	CleanupScope              CleanupScope                `json:"cleanup_scope,omitempty"`
	StructuralReason          string                      `json:"structural_reason,omitempty"`
	ChecksumCount             int                         `json:"checksum_count,omitempty"`
	SizeCount                 int                         `json:"size_count,omitempty"`
	LegacyURLTemplateDetected bool                        `json:"legacy_url_template_detected,omitempty"`
	Records                   []StorageCleanupRecordAudit `json:"records,omitempty"`
}

type StorageCleanupReport struct {
	Organization      string                  `json:"organization"`
	Project           string                  `json:"project"`
	PathPrefix        string                  `json:"path_prefix,omitempty"`
	Scanned           int                     `json:"scanned"`
	ScannedPaths      int                     `json:"scanned_paths,omitempty"`
	ClassifiedPaths   int                     `json:"classified_paths,omitempty"`
	UnclassifiedPaths int                     `json:"unclassified_paths,omitempty"`
	Summary           map[FindingKind]int     `json:"summary,omitempty"`
	Findings          []StorageCleanupFinding `json:"findings,omitempty"`
}

type StorageCleanupAuditRequest struct {
	Organization  string   `json:"organization"`
	Project       string   `json:"project"`
	PathPrefix    string   `json:"path_prefix,omitempty"`
	ExpectedPaths []string `json:"expected_paths,omitempty"`
	SelectedPaths []string `json:"selected_paths,omitempty"`
	CheckStorage  *bool    `json:"check_storage,omitempty"`
}

type StorageCleanupApplyRequest struct {
	Organization          string        `json:"organization"`
	Project               string        `json:"project"`
	PathPrefix            string        `json:"path_prefix,omitempty"`
	ExpectedPaths         []string      `json:"expected_paths,omitempty"`
	DeleteStaleDuplicates bool          `json:"delete_stale_duplicates"`
	DeleteRepoOrphans     bool          `json:"delete_repo_orphans"`
	DryRun                bool          `json:"dry_run"`
	SelectedPaths         []string      `json:"selected_paths,omitempty"`
	SelectedObjectIDs     []string      `json:"selected_object_ids,omitempty"`
	SelectedFindingKinds  []FindingKind `json:"selected_finding_kinds,omitempty"`
	CheckStorage          *bool         `json:"check_storage,omitempty"`
}

type StorageCleanupSkipped struct {
	NormalizedPath string      `json:"normalized_path"`
	ObjectID       string      `json:"object_id,omitempty"`
	Kind           FindingKind `json:"kind"`
	Reason         string      `json:"reason"`
}

type StorageCleanupStoragePurgeResult struct {
	ObjectID string `json:"object_id"`
	Purged   bool   `json:"purged"`
	Message  string `json:"message,omitempty"`
}

type StorageCleanupApplyResult struct {
	Report              StorageCleanupReport               `json:"report"`
	DryRun              bool                               `json:"dry_run"`
	DeletedRecordIDs    []string                           `json:"deleted_record_ids,omitempty"`
	StoragePurgeResults []StorageCleanupStoragePurgeResult `json:"storage_purge_results,omitempty"`
	RepoDeletePaths     []string                           `json:"repo_delete_paths,omitempty"`
	Skipped             []StorageCleanupSkipped            `json:"skipped,omitempty"`
}
