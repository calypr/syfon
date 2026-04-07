package upload

import (
	"context"
	"fmt"
	// Added for io.Reader
	"os"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// GeneratePresignedURL handles both Shepherd and Fence fallback
func GeneratePresignedUploadURL(ctx context.Context, bk transfer.Uploader, filename string, metadata common.FileMetadata, bucket string) (*common.PresignedURLResponse, error) {
	url, err := bk.ResolveUploadURL(ctx, "", filename, metadata, bucket)
	if err != nil {
		return nil, err
	}
	var res common.PresignedURLResponse
	res = common.PresignedURLResponse{URL: url, GUID: ""}
	return &res, nil
}

// GenerateUploadRequest helps preparing the HTTP request for upload and the progress bar for single part upload
func generateUploadRequest(ctx context.Context, bk transfer.Uploader, req common.FileUploadRequestObject, file *os.File, p *mpb.Progress) (common.FileUploadRequestObject, error) {
	if req.PresignedURL == "" {
		url, err := bk.ResolveUploadURL(ctx, req.GUID, req.ObjectKey, req.FileMetadata, req.Bucket)
		if err != nil {
			return req, fmt.Errorf("Upload error: %w", err)
		}
		req.PresignedURL = url
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
				decor.Name(req.ObjectKey, decor.WC{W: len(req.ObjectKey) + 1, C: decor.DindentRight}),
				decor.CountersKibiByte("% .2f / % .2f"),
			),
			mpb.AppendDecorators(
				decor.OnComplete(decor.Percentage(decor.WC{W: 5}), "done"),
			),
		)
	}

	return req, nil
}
