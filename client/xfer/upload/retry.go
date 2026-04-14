package upload

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
)

// GetWaitTime calculates exponential backoff with cap
func GetWaitTime(retryCount int) time.Duration {
	exp := 1 << retryCount // 2^retryCount
	seconds := int64(exp)
	if seconds > common.MaxWaitTime {
		seconds = common.MaxWaitTime
	}
	return time.Duration(seconds) * time.Second
}

// RetryFailedUploads re-uploads previously failed files with exponential backoff
func RetryFailedUploads(ctx context.Context, bk xfer.Uploader, logger xfer.TransferLogger, failedMap map[string]common.RetryObject) {
	if len(failedMap) == 0 {
		logger.Println("No failed files to retry.")
		return
	}

	sb := logger.Scoreboard()

	logger.Printf("Starting retry-upload for %d failed Uploads", len(failedMap))
	retryChan := make(chan common.RetryObject, len(failedMap))

	// Queue only non-already-succeeded files
	for _, ro := range failedMap {
		retryChan <- ro
	}

	if len(retryChan) == 0 {
		logger.Println("All previously failed files have since succeeded.")
		return
	}

	for ro := range retryChan {
		ro.RetryCount++
		logger.Printf("#%d retry — %s\n", ro.RetryCount, ro.SourcePath)
		wait := GetWaitTime(ro.RetryCount)
		logger.Printf("Waiting %.0f seconds before retry...\n", wait.Seconds())
		time.Sleep(wait)

		// Clean up old record if exists
		if ro.GUID != "" {
			if msg, err := bk.DeleteFile(
				ctx,
				ro.GUID,
			); err == nil {
				logger.Println(msg)
			}
		}

		file, err := os.Open(ro.SourcePath)
		if err != nil {
			continue
		}

		// Ensure filename is set
		if ro.ObjectKey == "" {
			absPath, _ := common.GetAbsolutePath(ro.SourcePath)
			ro.ObjectKey = filepath.Base(absPath)
		}

		if ro.Multipart {
			// Retry multipart
			err = MultipartUpload(ctx, bk, ro.SourcePath, ro.ObjectKey, ro.GUID, ro.Bucket, ro.FileMetadata, file, true)
			if err == nil {
				logger.Succeeded(ro.SourcePath, ro.GUID)
				if sb != nil {
					sb.IncrementSB(ro.RetryCount - 1)
				}
				continue
			}
		} else {
			// Retry single-part
			respURL, err := GeneratePresignedUploadURL(ctx, bk, ro.ObjectKey, ro.FileMetadata, ro.Bucket)
			if err != nil {
				handleRetryFailure(ctx, bk, logger, ro, retryChan, err)
				continue
			}

			stat, err := file.Stat()
			file.Close()
			if err != nil {
				handleRetryFailure(ctx, bk, logger, ro, retryChan, err)
				continue
			}

			if stat.Size() > common.FileSizeLimit {
				ro.Multipart = true
				retryChan <- ro
				continue
			}

			file, err = os.Open(ro.SourcePath)
			if err != nil {
				handleRetryFailure(ctx, bk, logger, ro, retryChan, err)
				continue
			}
			err = bk.Upload(ctx, respURL, file, stat.Size())
			_ = file.Close()
			if err == nil {
				logger.Succeeded(ro.SourcePath, ro.GUID)
				if sb != nil {
					sb.IncrementSB(ro.RetryCount - 1)
				}
				continue
			}
		}

		// On failure, requeue if retries remain
		handleRetryFailure(ctx, bk, logger, ro, retryChan, err)
	}
}

// handleRetryFailure logs failure and requeues if retries remain
func handleRetryFailure(ctx context.Context, bk xfer.Uploader, logger xfer.TransferLogger, ro common.RetryObject, retryChan chan common.RetryObject, err error) {
	logger.Failed(ro.SourcePath, ro.ObjectKey, ro.FileMetadata, ro.GUID, ro.RetryCount, ro.Multipart)
	if err != nil {
		logger.Println("Retry error:", err)
	}

	if ro.RetryCount < common.MaxRetryCount {
		retryChan <- ro
		return
	}

	// Max retries reached — final cleanup
	if ro.GUID != "" {
		if msg, err := bk.DeleteFile(ctx, ro.GUID); err == nil {
			logger.Println("Cleaned up failed record:", msg)
		} else {
			logger.Println("Cleanup failed:", err)
		}
	}

	if sb := logger.Scoreboard(); sb != nil {
		sb.IncrementSB(common.MaxRetryCount + 1)
	}

	if len(retryChan) == 0 {
		close(retryChan)
	}
}
