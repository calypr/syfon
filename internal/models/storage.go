package models

import (
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

// PendingLFSMeta stores a staged LFS metadata packet keyed by object checksum.
// It is submitted before transfer and consumed at verify-time.
type PendingLFSMeta struct {
	OID       string
	Candidate drs.DrsObjectCandidate
	CreatedAt time.Time
	ExpiresAt time.Time
}

// DrsObjectRecord mirrors the subset of drs_object columns returned by storage queries.
type DrsObjectRecord struct {
	ID               string
	Size             int64
	CreatedTime      time.Time
	UpdatedTime      time.Time
	DownloadCount    int64
	LastDownloadTime *time.Time
	Name             string
	Version          string
	Description      string
}
