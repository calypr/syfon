package core

import (
	"context"

	"github.com/calypr/syfon/internal/usage"
)

func (m *ObjectManager) RecordDownload(ctx context.Context, id string) error {
	return m.fileCounters.RecordFileDownload(ctx, id)
}

func (m *ObjectManager) RecordUpload(ctx context.Context, id string) error {
	return m.fileCounters.RecordFileUpload(ctx, id)
}

func (m *ObjectManager) RecordTransferAttributionEvents(ctx context.Context, events []usage.Event) error {
	return m.transferEvents.RecordTransferAttributionEvents(ctx, events)
}

func (m *ObjectManager) RecordProviderTransferEvents(ctx context.Context, events []usage.ProviderEvent) error {
	return m.providerEvents.RecordProviderTransferEvents(ctx, events)
}
