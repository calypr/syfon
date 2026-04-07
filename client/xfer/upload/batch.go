package upload

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/transfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func InitBatchUploadChannels(numParallel int, inputSliceLen int) (int, chan *http.Response, chan error, []common.FileUploadRequestObject) {
	workers := numParallel
	if workers < 1 || workers > inputSliceLen {
		workers = inputSliceLen
	}
	if workers < 1 {
		workers = 1
	}

	respCh := make(chan *http.Response, inputSliceLen)
	errCh := make(chan error, inputSliceLen)
	batchSlice := make([]common.FileUploadRequestObject, 0, workers)

	return workers, respCh, errCh, batchSlice
}

func BatchUpload(
	ctx context.Context,
	bk transfer.Uploader,
	logger *logs.Gen3Logger,
	furObjects []common.FileUploadRequestObject,
	workers int,
	respCh chan *http.Response,
	errCh chan error,
	bucketName string,
) {
	if len(furObjects) == 0 {
		return
	}

	// Ensure bucket is set
	for i := range furObjects {
		if furObjects[i].Bucket == "" {
			furObjects[i].Bucket = bucketName
		}
	}

	progress := mpb.New(mpb.WithOutput(os.Stdout))

	workCh := make(chan common.FileUploadRequestObject, len(furObjects))

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fur := range workCh {
				// --- Ensure presigned URL ---
				if fur.PresignedURL == "" {
					resp, err := GeneratePresignedUploadURL(ctx, bk, fur.ObjectKey, fur.FileMetadata, fur.Bucket)
					if err != nil {
						logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, "", 0, false)
						errCh <- err
						continue
					}
					fur.PresignedURL = resp.URL
					fur.GUID = resp.GUID
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, resp.GUID, 0, false) // update log
				}

				// --- Open file ---
				file, err := os.Open(fur.SourcePath)
				if err != nil {
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, fur.GUID, 0, false)
					errCh <- fmt.Errorf("file open error: %w", err)
					continue
				}

				fi, err := file.Stat()
				if err != nil {
					file.Close()
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, fur.GUID, 0, false)
					errCh <- fmt.Errorf("file stat error: %w", err)
					continue
				}

				if fi.Size() > common.FileSizeLimit {
					file.Close()
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, fur.GUID, 0, false)
					errCh <- fmt.Errorf("file size exceeds limit: %s", fur.ObjectKey)
					continue
				}

				// --- Progress bar ---
				bar := progress.AddBar(fi.Size(),
					mpb.PrependDecorators(
						decor.Name(fur.ObjectKey+" "),
						decor.CountersKibiByte("% .1f / % .1f"),
					),
					mpb.AppendDecorators(
						decor.Percentage(),
						decor.AverageSpeed(decor.SizeB1024(0), " % .1f"),
					),
				)

				proxyReader := bar.ProxyReader(file)

				// --- Upload ---
				err = bk.Upload(ctx, fur.PresignedURL, proxyReader, fi.Size())

				// Cleanup
				file.Close()
				bar.Abort(false)

				if err != nil {
					logger.Failed(fur.SourcePath, fur.ObjectKey, fur.FileMetadata, fur.GUID, 0, false)
					errCh <- err
					continue
				}

				// Success
				logger.DeleteFromFailedLog(fur.SourcePath)
				logger.Succeeded(fur.SourcePath, fur.GUID)
				logger.Scoreboard().IncrementSB(0)
			}
		}()
	}

	for _, obj := range furObjects {
		workCh <- obj
	}
	close(workCh)

	wg.Wait()
	progress.Wait()
}
