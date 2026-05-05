package download

import (
	"context"
	"path/filepath"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/calypr/syfon/client/transfer/engine"
)

type DownloadOptions struct {
	MultipartThreshold int64
	ChunkSize          int64
	Concurrency        int
}

// DownloadToPath downloads a single object using the generic EngineDownloader.
func DownloadToPath(
	ctx context.Context,
	bk transfer.ReadBackend,
	guid string,
	dstPath string,
) error {
	opts := DownloadOptions{
		MultipartThreshold: int64(5 * common.GB),
		Concurrency:        8,
		ChunkSize:          64 * common.MB,
	}
	return DownloadToPathWithOptions(ctx, bk, guid, dstPath, opts)
}

func DownloadToPathWithOptions(
	ctx context.Context,
	bk transfer.ReadBackend,
	guid string,
	dstPath string,
	opts DownloadOptions,
) error {
	downloader := &engine.GenericDownloader{Source: bk}
	return downloader.Download(ctx, guid, dstPath, opts.Concurrency, opts.ChunkSize, opts.MultipartThreshold)
}

// DownloadSingleWithProgress preserves the old single-download test entrypoint while
// routing the actual transfer through the engine.
func DownloadSingleWithProgress(ctx context.Context, dc Resolver, bk transfer.ReadBackend, guid, downloadPath, filenameFormat string) error {
	logger := bk.Logger()
	info, err := GetFileInfo(ctx, dc, logger, guid, "https", downloadPath, filenameFormat, true, nil)
	if err != nil {
		return err
	}

	dstPath := filepath.Join(downloadPath, info.Name)
	opts := DownloadOptions{
		MultipartThreshold: 1 * common.GB,
		Concurrency:        2,
		ChunkSize:          64 * common.MB,
	}
	return DownloadToPathWithOptions(ctx, bk, guid, dstPath, opts)
}
