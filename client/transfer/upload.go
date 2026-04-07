package transfer

import (
	"context"
	"os"

	"github.com/calypr/syfon/client/pkg/common"
)

type UploadOptions struct {
	MultipartThreshold int64
	ChunkSize          int64
	Concurrency        int
}

func defaultUploadOptions() UploadOptions {
	return UploadOptions{
		MultipartThreshold: common.FileSizeLimit,
		ChunkSize:          64 * common.MB,
		Concurrency:        8,
	}
}

// Resolver capability provides identity translation.
type Resolver interface {
	Resolve(ctx context.Context, id string) (*ResolvedObject, error)
}

// UploadPlanner statically evaluates a file to determine the optimal transfer strategy.
type UploadPlanner struct {
	Options UploadOptions
}

func (p *UploadPlanner) Plan(ctx context.Context, fileSize int64) *TransferPlan {
	strategy := StrategySingleStream
	if fileSize >= p.Options.MultipartThreshold && p.Options.MultipartThreshold > 0 {
		strategy = StrategyMultipart
	}
	return &TransferPlan{
		Strategy:  strategy,
		TotalSize: fileSize,
	}
}

// Upload is the high-level Consumer API entry point for data movement.
// It relies on explicit Strategy Planning -> Execution logic mapping.
func Upload(
	ctx context.Context,
	resolver Resolver,
	writer ObjectWriter,
	req common.FileUploadRequestObject,
	showProgress bool,
	opts ...UploadOptions,
) error {
	options := defaultUploadOptions()
	if len(opts) > 0 {
		options = opts[0]
	}

	file, err := os.Open(req.SourcePath)
	if err != nil {
		return err
	}
	defer file.Close()
	fi, _ := file.Stat()

	// 1. Planning Phase (Strategy Selection)
	planner := &UploadPlanner{Options: options}
	plan := planner.Plan(ctx, fi.Size())

	// 2. Execution Phase (Orchestration mapping)
	if plan.Strategy == StrategyMultipart {
		return MultipartUploadManaged(ctx, resolver, req, file, options.ChunkSize)
	}
	return UploadSingle(ctx, writer, req.GUID, req.ObjectKey, file, showProgress)
}
