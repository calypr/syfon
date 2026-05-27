package upload

import (
	"context"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

// GeneratePresignedUploadURL resolves a signed upload URL using plain inputs.
func GeneratePresignedUploadURL(ctx context.Context, bk transfer.Uploader, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	return bk.ResolveUploadURL(ctx, "", filename, metadata, bucket)
}
