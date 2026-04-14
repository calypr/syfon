package upload

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func BatchUpload(
	ctx context.Context,
	bk xfer.Uploader,
	logger xfer.TransferLogger,
	uploadPath string,
	filePaths []string,
	workers int,
	bucketName string,
	includeSubDirName bool,
	hasMetadata bool,
) {
	if len(filePaths) == 0 {
		return
	}

	jobs := make([]uploadRequest, 0, len(filePaths))
	for _, filePath := range filePaths {
		src, key, metadata, err := ProcessFilename(logger, uploadPath, filePath, "", includeSubDirName, hasMetadata)
		if err != nil {
			logger.Failed(filePath, filePath, common.FileMetadata{}, "", 0, false)
			continue
		}
		jobs = append(jobs, uploadRequest{
			sourcePath: src,
			objectKey:  key,
			metadata:   metadata,
			bucket:     bucketName,
		})
	}
	if len(jobs) == 0 {
		return
	}

	progress := mpb.New(mpb.WithOutput(os.Stdout))
	workCh := make(chan uploadRequest, len(jobs))
	errCh := make(chan error, len(jobs))
	respCh := make(chan *http.Response, len(jobs))
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range workCh {
				if job.bucket == "" {
					job.bucket = bucketName
				}
				file, err := os.Open(job.sourcePath)
				if err != nil {
					logger.Failed(job.sourcePath, job.objectKey, job.metadata, job.guid, 0, false)
					errCh <- fmt.Errorf("file open error: %w", err)
					continue
				}

				fi, err := file.Stat()
				if err != nil {
					file.Close()
					logger.Failed(job.sourcePath, job.objectKey, job.metadata, job.guid, 0, false)
					errCh <- fmt.Errorf("file stat error: %w", err)
					continue
				}

				bar := progress.AddBar(fi.Size(),
					mpb.PrependDecorators(
						decor.Name(job.objectKey+" "),
						decor.CountersKibiByte("% .1f / % .1f"),
					),
					mpb.AppendDecorators(
						decor.Percentage(),
						decor.AverageSpeed(decor.SizeB1024(0), " % .1f"),
					),
				)

				proxyReader := bar.ProxyReader(file)
				url, err := bk.ResolveUploadURL(ctx, job.guid, job.objectKey, job.metadata, job.bucket)
				if err != nil {
					file.Close()
					bar.Abort(false)
					logger.Failed(job.sourcePath, job.objectKey, job.metadata, job.guid, 0, false)
					errCh <- err
					continue
				}
				err = bk.Upload(ctx, url, proxyReader, fi.Size())
				file.Close()
				bar.Abort(false)
				if err != nil {
					logger.Failed(job.sourcePath, job.objectKey, job.metadata, job.guid, 0, false)
					errCh <- err
					continue
				}
				logger.DeleteFromFailedLog(job.sourcePath)
				logger.Succeeded(job.sourcePath, job.guid)
				if sb := logger.Scoreboard(); sb != nil {
					sb.IncrementSB(0)
				}
				respCh <- nil
			}
		}()
	}

	for _, job := range jobs {
		workCh <- job
	}
	close(workCh)

	wg.Wait()
	progress.Wait()
	_ = errCh
	_ = respCh
}
