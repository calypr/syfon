package upload

import (
	"context"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/calypr/syfon/client/transfer/engine"
)

// Upload is now a thin wrapper around the generic EngineUploader.
func Upload(ctx context.Context, bk transfer.Backend, sourcePath, objectKey, guid, bucket string, metadata common.FileMetadata, showProgress bool, forceMultipart bool) error {
	return UploadWithOptions(ctx, bk, sourcePath, objectKey, guid, bucket, metadata, showProgress, forceMultipart)
}

func UploadWithOptions(ctx context.Context, bk transfer.Backend, sourcePath, objectKey, guid, bucket string, metadata common.FileMetadata, showProgress bool, forceMultipart bool) error {
	req := transfer.TransferRequest{
		SourcePath:     sourcePath,
		ObjectKey:      objectKey,
		GUID:           guid,
		Bucket:         bucket,
		Metadata:       metadata,
		ForceMultipart: forceMultipart,
	}

	uploader := &engine.GenericUploader{Backend: bk}
	return uploader.Upload(ctx, req, showProgress)
}
