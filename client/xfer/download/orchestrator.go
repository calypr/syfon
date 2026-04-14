package download

import (
	"context"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
)

// DownloadFile is a high-level orchestrator that downloads a file using the provided backend.
func DownloadFile(ctx context.Context, dc drs.Client, bk xfer.Downloader, guid, destPath string) error {
	opts := DownloadOptions{
		MultipartThreshold: int64(5 * common.GB),
	}
	// Note: We could expose more options here if needed
	return DownloadToPathWithOptions(ctx, dc, bk, bk.Logger().Slog(), guid, destPath, "", opts)
}
