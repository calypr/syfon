package core

import (
	"context"

	"github.com/calypr/syfon/internal/storage"
)

// StorageAccess is the narrow storage capability used by core for signed
// access. Core owns this consumer port; storage owns the concrete values.
type StorageAccess interface {
	Access(context.Context, storage.AccessRequest) (storage.Access, error)
}

// StorageMultipart is the narrow storage capability used by core while the
// transfer workflow is still a transitional ObjectManager facade.
type StorageMultipart interface {
	BeginMultipart(context.Context, storage.ObjectTarget) (storage.UploadID, error)
	AccessMultipartPart(context.Context, storage.MultipartPartRequest) (storage.Access, error)
	CompleteMultipart(context.Context, storage.CompleteMultipartRequest) error
}

// StorageProbe is a batch object-probe capability. Probe results contain raw
// storage facts and provider errors; core maps them to its policy/status types.
type StorageProbe interface {
	Probe(context.Context, []storage.ProbeTarget) []storage.ProbeResult
}

// StorageInventory is the raw prefix inventory capability used by core's
// project inspection and validation policy.
type StorageInventory interface {
	Inventory(context.Context, storage.InventoryRequest) (storage.InventoryResult, error)
}

// StorageDelete is the exact physical-location deletion capability. Core is
// responsible for authorization, target selection, and cleanup orchestration.
type StorageDelete interface {
	DeleteExact(context.Context, []storage.DeleteTarget) error
}

// StoragePorts records the independent storage capabilities consumed by core.
// Production composition assigns the same *storage.Manager to each slot.
type StoragePorts struct {
	Access    StorageAccess
	Multipart StorageMultipart
	Probe     StorageProbe
	Inventory StorageInventory
	Delete    StorageDelete
}
