package runtime

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/calypr/syfon/client/xfer/download"
	// "github.com/calypr/syfon/client/xfer/upload"
	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/transfer"
)

// DataClient is the high-level simplified interface for data operations.
// It abstracts away resolution layers, multipart thresholds, and multi-cloud nuances.
type DataClient struct {
	api    drs.Client
	logger *logs.Gen3Logger
}

func New(api drs.Client, logger *logs.Gen3Logger) *DataClient {
	return &DataClient{
		api:    api,
		logger: logger,
	}
}

// Download automatically performs a parallel, multi-cloud download using server-signed
// URLs if the file is large, removing the need for local cloud credentials.
func (c *DataClient) Download(ctx context.Context, id, dest string) error {
	// Initialize the SignedURLBackend which uses the syfon server for part signing.
	// This fulfills the "No Local Credentials" requirement.
	backend := transfer.NewSignedURLBackend(c.api)

	// Delegate to the robust download orchestrator.
	opts := download.DownloadOptions{
		MultipartThreshold: 5 * 1024 * 1024, // 5MB default for parallel download
		Concurrency:        8,
	}

	return download.DownloadFile(ctx, c.api, backend, id, dest, opts)
}

// Upload handles single or multipart uploads, delegating resolution and signing to the server.
func (c *DataClient) Upload(ctx context.Context, src, metadata string) error {
	// WIP: Implementation for high-level Upload.
	return fmt.Errorf("high-level upload not yet implemented")
}

// Simple Entry Points for quick usage.

func Download(ctx context.Context, api drs.Client, id, dest string) error {
	logger := logs.NewGen3Logger(slog.Default(), "", "")
	return New(api, logger).Download(ctx, id, dest)
}
