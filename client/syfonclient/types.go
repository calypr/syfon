package syfonclient

import (
	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

// Options and request types for client services.

type DRSPage struct {
	DrsObjects []drsapi.DrsObject `json:"drs_objects"`
}

type DeleteByQueryOptions struct {
	Organization string
	ProjectID    string
	Hash         string
	HashType     string
}

type ListRecordsOptions struct {
	Hash         string
	URL          string
	Organization string
	ProjectID    string
	Limit        int
	Page         int
}

type UploadURLRequest struct {
	FileID    string
	Bucket    string
	FileName  string
	ExpiresIn int
}

type MetricsFilesOptions struct {
	Limit        int
	Offset       int
	InactiveDays int
	Organization string
	ProjectID    string
}

type MetricsSummaryOptions struct {
	InactiveDays int
	Organization string
	ProjectID    string
}
