package transfer

import (
	"context"
	"github.com/calypr/syfon/client/pkg/hash"
)

// ResolvedObject is the outcome of the Resolution layer (e.g. DRS).
// It contains everything the transfer engine needs to start planning.
type ResolvedObject struct {
	Id           string
	Name         string
	Size         int64
	Checksums    hash.HashInfo
	ProviderURL  string // The cloud-native URL or signed URL
	AccessMethod string // s3, gs, azblob, https, etc.
}

// TransferStrategy defines how the engine intends to move the data.
type TransferStrategy string

const (
	StrategySingleStream   TransferStrategy = "single_stream"
	StrategyParallelRanges TransferStrategy = "parallel_ranges"
	StrategyMultipart      TransferStrategy = "multipart"
)

// TransferPlan is the blueprint for a specific data movement.
// It tracks chunks, retries, and resumability state.
type TransferPlan struct {
	Strategy TransferStrategy
	TotalSize int64
	Chunks    []Chunk
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

// Planner is the interface for components that analyze a ResolvedObject 
// to decide on the best strategy.
type Planner interface {
	PlanDownload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
	PlanUpload(ctx context.Context, obj ResolvedObject) (*TransferPlan, error)
}
