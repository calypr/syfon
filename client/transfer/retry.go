package transfer

import (
	"context"
	"time"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
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

// RetryFailedUploads re-uploads previously failed files with exponential backoff.
// Modernized to use capability-based interfaces.
func RetryFailedUploads(
	ctx context.Context,
	resolver Resolver,
	writer ObjectWriter,
	deleter ObjectDeleter,
	logger *logs.Gen3Logger,
	failedMap map[string]common.RetryObject,
) {
	if len(failedMap) == 0 {
		return
	}

	logger.Printf("Starting retry-upload for %d failed Uploads", len(failedMap))
	
	for _, ro := range failedMap {
		ro.RetryCount++
		logger.Printf("#%d retry — %s\n", ro.RetryCount, ro.SourcePath)
		
		wait := GetWaitTime(ro.RetryCount)
		time.Sleep(wait)

		// 1. Cleanup old record if it exists
		if ro.GUID != "" {
			_ = deleter.Delete(ctx, ro.GUID)
		}

		// 2. Perform Upload
		req := common.FileUploadRequestObject{
			SourcePath:   ro.SourcePath,
			ObjectKey:    ro.ObjectKey,
			GUID:         ro.GUID,
			FileMetadata: ro.FileMetadata,
			Bucket:       ro.Bucket,
		}

		err := Upload(ctx, resolver, writer, req, true)
		if err == nil {
			logger.Succeeded(ro.SourcePath, ro.GUID)
			continue
		}

		// 3. Handle Failure: Log and requeue logic normally happens in a separate loop, 
		// but for simplicity in this refactor, we provide a unified entry point.
		logger.Failed(ro.SourcePath, ro.ObjectKey, ro.FileMetadata, ro.GUID, ro.RetryCount, ro.Multipart)
	}
}
