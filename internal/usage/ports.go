package usage

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/objects"
)

// FileCounterRecorder records object upload and download counters.
type FileCounterRecorder interface {
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
}

// FileCounterStore is the persistence capability used by the usage ingest
// service for completed object uploads and downloads.
//
// It is intentionally separate from the report stores: a backend may expose
// all of these methods, but callers should receive only the capability they
// need.
type FileCounterStore interface {
	FileCounterRecorder
}

// TransferEventWriter records access-issued events assembled by the transfers
// workflow. The usage package owns Event and its identity; the workflow owns
// the projection from an access request to that value.
type TransferEventWriter interface {
	RecordTransferAttributionEvents(ctx context.Context, events []Event) error
}

// TransferEventStore is the persistence capability for access-issued events.
type TransferEventStore interface {
	TransferEventWriter
}

// ProviderEventStore is the persistence capability for provider-reported
// events. Matching and reconciliation remain in the persistence adapter.
type ProviderEventStore interface {
	ProviderEventRecorder
}

// IngestStore contains the independent write capabilities used by Service.
// Keeping the method sets named here lets composition pass a concrete backend
// without making the usage service depend on that backend's wider API.
type IngestStore interface {
	FileCounterStore
	TransferEventStore
	ProviderEventStore
}

// Ingestor is the narrow write view exposed to workflows and HTTP adapters.
type Ingestor interface {
	FileCounterRecorder
	TransferEventWriter
	ProviderEventRecorder
}

// FileUsageReader reads per-object usage and unscoped reports.
type FileUsageReader interface {
	GetFileUsage(ctx context.Context, objectID string) (*FileUsage, error)
	ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (FileUsageSummary, error)
}

// FileUsageStore is the persistence capability for file accounting reads.
// Scoped query methods remain optional optimizations below.
type FileUsageStore interface {
	FileUsageReader
}

// ObjectReader reads objects for metrics authorization and fallback reports.
// requiredMethod is supplied by callers so the object service can enforce the
// same access method as the existing metrics paths.
type ObjectReader interface {
	GetObject(ctx context.Context, ident, requiredMethod string) (*objects.Record, error)
	ListObjectIDsByScope(ctx context.Context, organization, project, requiredMethod string) ([]string, error)
}

// ProviderEventRecorder records provider-reported transfer events.
type ProviderEventRecorder interface {
	RecordProviderTransferEvents(ctx context.Context, events []ProviderEvent) error
}

// TransferQuery reads transfer attribution reports.
type TransferQuery interface {
	GetTransferAttributionSummary(ctx context.Context, filter Filter) (Summary, error)
	GetTransferAttributionBreakdown(ctx context.Context, filter Filter, groupBy string) ([]Breakdown, error)
}

// TransferReportStore is the persistence capability for transfer reports.
// Scoped query methods remain optional optimizations below.
type TransferReportStore interface {
	TransferQuery
}

// ReportStore is the complete persistence report capability consumed by
// Service. File and transfer stores are named separately so future
// composition can split them without changing the use-case boundary.
type ReportStore interface {
	FileUsageStore
	TransferReportStore
}

// OptionalScopedFileUsageQuery is an optional optimization for authorized
// metrics queries. Callers retain a per-object fallback when it is unavailable.
type OptionalScopedFileUsageQuery interface {
	ListFileUsagePageByScope(ctx context.Context, organization, project string, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	ListFileUsagePageByResources(ctx context.Context, resources []string, includeUnscoped bool, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummaryByScope(ctx context.Context, organization, project string, inactiveSince *time.Time) (FileUsageSummary, error)
	GetFileUsageSummaryByResources(ctx context.Context, resources []string, includeUnscoped bool, inactiveSince *time.Time) (FileUsageSummary, error)
	GetProjectRecordSummaryByScope(ctx context.Context, organization, project string) (FileUsageSummary, error)
}

// OptionalScopedTransferQuery is an optional optimization for authorized
// transfer reports. Callers retain a per-scope fallback when it is unavailable.
type OptionalScopedTransferQuery interface {
	GetTransferAttributionSummaryByResources(ctx context.Context, filter Filter, resources []string) (Summary, error)
	GetTransferAttributionBreakdownByResources(ctx context.Context, filter Filter, groupBy string, resources []string) ([]Breakdown, error)
}
