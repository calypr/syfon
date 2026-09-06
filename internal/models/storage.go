package models

import (
	"time"

	"github.com/calypr/syfon/internal/objects"
)

// PendingLFSMeta stores a staged LFS metadata packet keyed by object checksum.
// It is submitted before transfer and consumed at verify-time.
type PendingLFSMeta struct {
	OID       string
	Candidate objects.Candidate
	CreatedAt time.Time
	ExpiresAt time.Time
}
