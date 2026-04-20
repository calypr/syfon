package upload

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

func UploadSingle(ctx context.Context, bk transfer.Uploader, logger transfer.TransferLogger, sourcePath, objectKey, guid, bucket string, metadata common.FileMetadata, showProgress bool) error {
	req := uploadRequest{
		sourcePath: sourcePath,
		objectKey:  objectKey,
		guid:       guid,
		bucket:     bucket,
		metadata:   metadata,
	}

	file, err := os.Open(req.sourcePath)
	if err != nil {
		logger.Failed(req.sourcePath, req.objectKey, common.FileMetadata{}, "", 0, false)
		return fmt.Errorf("error opening file %s: %w", req.sourcePath, err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	fur, err := generateUploadRequest(ctx, bk, req, file, nil)
	if err != nil {
		logger.Failed(req.sourcePath, req.objectKey, common.FileMetadata{}, guid, 0, false)
		return err
	}

	reader := io.Reader(file)
	if progress := common.GetProgress(ctx); progress != nil {
		reader = newProgressReader(file, progress, resolveUploadOID(fur.objectKey, fur.guid), fi.Size())
	}

	err = bk.Upload(ctx, fur.presignedURL, reader, fi.Size())
	if err != nil {
		return err
	}

	logger.Succeeded(req.sourcePath, guid)
	return nil
}
