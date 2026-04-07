package upload

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/transfer"
)

func UploadSingle(ctx context.Context, bk transfer.Uploader, logger *logs.Gen3Logger, req common.FileUploadRequestObject, showProgress bool) error {
	logger.DebugContext(ctx, "File upload request",
		"source_path", req.SourcePath,
		"object_key", req.ObjectKey,
		"guid", req.GUID,
		"bucket", req.Bucket,
	)

	// Helper to handle * in path if it was passed, though optimally caller handles this.
	// We will trust the SourcePath in the request object mostly, but for safety we can check existence.
	// But commonly parsing happens before creating the object usually.
	// Let's assume req.SourcePath is a single valid file path for now as per design.

	file, err := os.Open(req.SourcePath)
	if err != nil {
		if showProgress {
			sb := logger.Scoreboard()
			if sb != nil {
				sb.IncrementSB(len(sb.Counts))
				sb.PrintSB()
			}
		}
		logger.Failed(req.SourcePath, req.ObjectKey, common.FileMetadata{}, "", 0, false)
		logger.ErrorContext(ctx, "File open error", "file", req.SourcePath, "error", err)
		return fmt.Errorf("[ERROR] when opening file path %s, an error occurred: %s\n", req.SourcePath, err.Error())
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := fi.Size()

	furObject, err := generateUploadRequest(ctx, bk, req, file, nil)
	if err != nil {
		if showProgress {
			sb := logger.Scoreboard()
			if sb != nil {
				sb.IncrementSB(len(sb.Counts))
				sb.PrintSB()
			}
		}
		logger.Failed(req.SourcePath, req.ObjectKey, common.FileMetadata{}, req.GUID, 0, false)
		logger.ErrorContext(ctx, "Error occurred during request generation", "file", req.SourcePath, "error", err)
		return fmt.Errorf("[ERROR] Error occurred during request generation for file %s: %s\n", req.SourcePath, err.Error())
	}

	progressCallback := common.GetProgress(ctx)
	oid := common.GetOid(ctx)
	if oid == "" {
		oid = resolveUploadOID(furObject)
	}

	var reader io.Reader = file
	var progressTracker *progressReader
	if progressCallback != nil {
		progressTracker = newProgressReader(file, progressCallback, oid, fileSize)
		reader = progressTracker
	}

	err = bk.Upload(ctx, furObject.PresignedURL, reader, fileSize)
	if progressTracker != nil {
		if finalizeErr := progressTracker.Finalize(); finalizeErr != nil && err == nil {
			err = finalizeErr
		}
	}

	if err != nil {
		logger.ErrorContext(ctx, "Upload failed", "error", err)
		return err
	}

	logger.DebugContext(ctx, "Successfully uploaded", "file", req.ObjectKey)
	logger.Succeeded(req.SourcePath, req.GUID)

	if showProgress {
		sb := logger.Scoreboard()
		if sb != nil {
			sb.IncrementSB(0)
			sb.PrintSB()
		}
	}
	return nil
}
