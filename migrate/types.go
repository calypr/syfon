package migrate

import (
	"context"
	"time"

	"github.com/calypr/syfon/apigen/server/drs"
)

// IndexdRecord is the source record shape returned by Gen3 Indexd APIs.
// Deprecated Indexd fields are accepted so source payloads decode cleanly,
// but they are intentionally not loaded into Syfon.
type IndexdRecord struct {
	DID                string            `json:"did"`
	Size               *int64            `json:"size,omitempty"`
	FileName           *string           `json:"file_name,omitempty"`
	Version            *string           `json:"version,omitempty"`
	Description        *string           `json:"description,omitempty"`
	URLs               []string          `json:"urls,omitempty"`
	Hashes             map[string]string `json:"hashes,omitempty"`
	Authz              []string          `json:"authz,omitempty"`
	CreatedDate        *string           `json:"created_date,omitempty"`
	UpdatedDate        *string           `json:"updated_date,omitempty"`
	CreatedTime        *string           `json:"created_time,omitempty"`
	UpdatedTime        *string           `json:"updated_time,omitempty"`
	ContentCreatedDate *string           `json:"content_created_date,omitempty"`
	ContentUpdatedDate *string           `json:"content_updated_date,omitempty"`
	Aliases            []string          `json:"aliases,omitempty"`
	Form               *string           `json:"form,omitempty"`

	Baseid       *string                `json:"baseid,omitempty"`
	Rev          *string                `json:"rev,omitempty"`
	Uploader     *string                `json:"uploader,omitempty"`
	ACL          []string               `json:"acl,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	URLsMetadata map[string]interface{} `json:"urls_metadata,omitempty"`
}

type IndexdPage struct {
	Records []IndexdRecord `json:"records"`
	IDs     []string       `json:"ids"`
	Start   string         `json:"start"`
	Page    *int           `json:"page,omitempty"`
	Limit   *int           `json:"limit,omitempty"`
}

type SourceLister interface {
	ListPage(ctx context.Context, limit int, start string) ([]IndexdRecord, error)
}

type Loader interface {
	LoadBatch(ctx context.Context, records []MigrationRecord) error
}

type PrivilegeLister interface {
	UserPrivileges(ctx context.Context) (map[string]map[string]bool, error)
}

type DumpReader interface {
	ReadBatches(ctx context.Context, batchSize int, fn func([]MigrationRecord) error) error
}

type MigrationRecord struct {
	ID               string             `json:"id"`
	Name             *string            `json:"name,omitempty"`
	Size             int64              `json:"size"`
	Version          *string            `json:"version,omitempty"`
	Description      *string            `json:"description,omitempty"`
	CreatedTime      time.Time          `json:"created_time"`
	UpdatedTime      *time.Time         `json:"updated_time,omitempty"`
	Checksums        []drs.Checksum     `json:"checksums"`
	AccessMethods    []drs.AccessMethod `json:"access_methods,omitempty"`
	ControlledAccess []string           `json:"controlled_access,omitempty"`
	Authz            []string           `json:"authz,omitempty"`
}

type MigrationRequest struct {
	Records []MigrationRecord `json:"records"`
}

type Stats struct {
	Fetched          int
	Transformed      int
	Loaded           int
	Skipped          int
	Errors           int
	CountOfUniqueIDs int
}

type Config struct {
	BatchSize    int
	Limit        int
	DryRun       bool
	DefaultAuthz []string
	Sweeps       int
}
