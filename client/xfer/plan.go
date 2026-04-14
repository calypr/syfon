package xfer

import (
	"context"

	"github.com/calypr/syfon/client/pkg/hash"
)

// ResolvedObject is the outcome of the resolution layer.
type ResolvedObject struct {
	Id           string
	Name         string
	Size         int64
	Checksums    hash.HashInfo
	ProviderURL  string
	AccessMethod string
}

// TransferStrategy defines how the engine intends to move the data.
type TransferStrategy string

const (
	StrategySingleStream   TransferStrategy = "single_stream"
	StrategyParallelRanges TransferStrategy = "parallel_ranges"
	StrategyMultipart      TransferStrategy = "multipart"
)

// TransferPlan is the blueprint for a specific data movement.
type TransferPlan struct {
	Strategy       TransferStrategy
	TotalSize      int64
	Chunks         []Chunk
	CheckpointPath string
}

type Chunk struct {
	Index  int
	Offset int64
	Length int64
	Status ChunkStatus
}

type ChunkStatus string

const (
	ChunkPending   ChunkStatus = "pending"
	ChunkCompleted ChunkStatus = "completed"
	ChunkFailed    ChunkStatus = "failed"
)

// Planner defines how to analyze a resolved object into a transfer plan.
type Planner interface {
	PlanDownload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
	PlanUpload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
}
