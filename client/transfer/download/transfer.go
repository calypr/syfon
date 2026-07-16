package download

import (
	"context"

	"github.com/calypr/syfon/client/transfer"
	"github.com/calypr/syfon/client/transfer/engine"
)

type DownloadOptions struct {
	MultipartThreshold int64
	ChunkSize          int64
	Concurrency        int
	RetryStrategy      transfer.RetryStrategy
}

func DownloadToPathWithOptions(
	ctx context.Context,
	bk transfer.ReadBackend,
	guid string,
	dstPath string,
	opts DownloadOptions,
) error {
	downloader := &engine.GenericDownloader{Source: bk, RetryStrategy: opts.RetryStrategy}
	return downloader.Download(ctx, guid, dstPath, opts.Concurrency, opts.ChunkSize, opts.MultipartThreshold)
}
