package upload

import (
	"context"
	"fmt"
	"os"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// GeneratePresignedUploadURL resolves a signed upload URL using plain inputs.
func GeneratePresignedUploadURL(ctx context.Context, bk xfer.Uploader, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	return bk.ResolveUploadURL(ctx, "", filename, metadata, bucket)
}

func generateUploadRequest(ctx context.Context, bk xfer.Uploader, req uploadRequest, file *os.File, p *mpb.Progress) (uploadRequest, error) {
	if req.presignedURL == "" {
		url, err := bk.ResolveUploadURL(ctx, req.guid, req.objectKey, req.metadata, req.bucket)
		if err != nil {
			return req, fmt.Errorf("upload error: %w", err)
		}
		req.presignedURL = url
	}

	fi, err := file.Stat()
	if err != nil {
		return req, fmt.Errorf("stat failed: %w", err)
	}

	if fi.Size() > common.FileSizeLimit {
		return req, fmt.Errorf("file size exceeds limit")
	}

	if p != nil {
		p.AddBar(fi.Size(),
			mpb.PrependDecorators(
				decor.Name(req.objectKey, decor.WC{W: len(req.objectKey) + 1, C: decor.DindentRight}),
				decor.CountersKibiByte("% .2f / % .2f"),
			),
			mpb.AppendDecorators(
				decor.OnComplete(decor.Percentage(decor.WC{W: 5}), "done"),
			),
		)
	}

	return req, nil
}
