package download

import (
	"context"
	"io"

	"github.com/calypr/syfon/client/transfer"
)

// GetDownloadReader prepares a reader for the requested file, optionally with a range.
func GetDownloadReader(ctx context.Context, bk transfer.ReadBackend, fdr *downloadRequest) (io.ReadCloser, error) {
	if fdr.rangeBytes > 0 || (fdr.rangeStart != nil || fdr.rangeEnd != nil) {
		offset := int64(0)
		if fdr.rangeStart != nil {
			offset = *fdr.rangeStart
		} else if fdr.rangeBytes > 0 {
			offset = fdr.rangeBytes
		}

		length := int64(-1)
		if fdr.rangeEnd != nil {
			length = (*fdr.rangeEnd - offset) + 1
		}

		return bk.GetRangeReader(ctx, fdr.guid, offset, length)
	}

	return bk.GetReader(ctx, fdr.guid)
}
