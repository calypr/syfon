package usage

import (
	"context"
	"time"
)

// FileCounterRecorder records object upload and download counters.
type FileCounterRecorder interface {
	RecordFileUpload(ctx context.Context, objectID string) error
	RecordFileDownload(ctx context.Context, objectID string) error
}

// FileUsageReader reads per-object usage and unscoped reports.
type FileUsageReader interface {
	GetFileUsage(ctx context.Context, objectID string) (*FileUsage, error)
	ListFileUsageByObjectIDs(ctx context.Context, ids []string) ([]FileUsage, error)
	ListFileUsage(ctx context.Context, limit, offset int, inactiveSince *time.Time) ([]FileUsage, error)
	GetFileUsageSummary(ctx context.Context, inactiveSince *time.Time) (FileUsageSummary, error)
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
