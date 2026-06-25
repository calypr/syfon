package repair

import "time"

const (
	FindingDuplicateSyfonPaths FindingKind = "duplicate_syfon_paths"
	FindingSyfonMissingInRepo  FindingKind = "syfon_missing_in_repo"
	FindingRepoMissingInSyfon  FindingKind = "repo_missing_in_syfon"
)

type ProjectDiffAuditRequest struct {
	Organization  string   `json:"organization"`
	Project       string   `json:"project"`
	PathPrefix    string   `json:"path_prefix,omitempty"`
	ExpectedPaths []string `json:"expected_paths,omitempty"`
}

type ProjectDiffFinding struct {
	Kind              FindingKind `json:"kind"`
	Severity          Severity    `json:"severity"`
	NormalizedPath    string      `json:"normalized_path"`
	ObjectIDs         []string    `json:"object_ids,omitempty"`
	RecordCount       int         `json:"record_count,omitempty"`
	SizeBytes         int64       `json:"size_bytes,omitempty"`
	DownloadCount     int64       `json:"download_count,omitempty"`
	LastDownloadTime  *time.Time  `json:"last_download_time,omitempty"`
	RecommendedAction string      `json:"recommended_action,omitempty"`
}

type ProjectDiffSummary struct {
	CountsByKind         map[FindingKind]int `json:"counts_by_kind,omitempty"`
	TotalFindings        int                 `json:"total_findings"`
	IndexedPathCount     int                 `json:"indexed_path_count"`
	ExpectedPathCount    int                 `json:"expected_path_count"`
	MatchedPathCount     int                 `json:"matched_path_count"`
	IncludesRepoManifest bool                `json:"includes_repo_manifest"`
	ScannedRecordCount   int                 `json:"scanned_record_count"`
}

type ProjectDiffReport struct {
	Organization string               `json:"organization"`
	Project      string               `json:"project"`
	PathPrefix   string               `json:"path_prefix,omitempty"`
	Summary      ProjectDiffSummary   `json:"summary"`
	Findings     []ProjectDiffFinding `json:"findings,omitempty"`
}
