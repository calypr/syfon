package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"
)

// downloadFiles performs bounded parallel downloads and collects ALL errors
func downloadFiles(
	ctx context.Context,
	bk xfer.Downloader,
	files []downloadRequest,
	numParallel int,
	protocol string,
) (int, error) {
	if len(files) == 0 {
		return 0, nil
	}

	logger := bk.Logger()

	// Scoreboard: maxRetries = 0 for now (no retry logic yet)
	sb := logger.Scoreboard()
	if sb == nil {
		sb = logs.NewSB(0, logger.Slog())
	}

	progress := common.GetProgress(ctx)
	useProgressBars := (progress == nil)

	var p *mpb.Progress
	if useProgressBars {
		p = mpb.New(mpb.WithOutput(os.Stdout))
	}

	var eg errgroup.Group
	eg.SetLimit(numParallel)

	var success atomic.Int64
	var mu sync.Mutex
	var allErrors []error

	for i := range files {
		fdr := &files[i] // capture loop variable

		eg.Go(func() error {
			var err error

			defer func() {
				if err != nil {
					// Final failure bucket
					sb.IncrementSB(len(sb.Counts) - 1)

					mu.Lock()
					allErrors = append(allErrors, err)
					mu.Unlock()
				} else {
					success.Add(1)
					sb.IncrementSB(0) // success, no retries
				}
			}()

			// Get presigned URL
			if err = GetDownloadResponse(ctx, bk, fdr, protocol); err != nil {
				err = fmt.Errorf("get URL for %s (GUID: %s): %w", fdr.filename, fdr.guid, err)
				return err
			}

			// Prepare directories
			fullPath := filepath.Join(fdr.downloadPath, fdr.filename)
			if dir := filepath.Dir(fullPath); dir != "." {
				if err = os.MkdirAll(dir, 0766); err != nil {
					_ = fdr.response.Body.Close()
					err = fmt.Errorf("mkdir for %s: %w", fullPath, err)
					return err
				}
			}

			flags := os.O_CREATE | os.O_WRONLY
			if fdr.rangeBytes > 0 {
				flags |= os.O_APPEND
			} else if fdr.overwrite {
				flags |= os.O_TRUNC
			}

			file, err := os.OpenFile(fullPath, flags, 0666)
			if err != nil {
				_ = fdr.response.Body.Close()
				err = fmt.Errorf("open local file %s: %w", fullPath, err)
				return err
			}

			// Progress bar for this file
			total := fdr.response.ContentLength + fdr.rangeBytes
			var writer io.Writer = file
			var bar *mpb.Bar
			var tracker *progressWriter

			if useProgressBars {
				bar = p.AddBar(total,
					mpb.PrependDecorators(
						decor.Name(truncateFilename(fdr.filename, 40)+" "),
						decor.CountersKibiByte("% .1f / % .1f"),
					),
					mpb.AppendDecorators(
						decor.Percentage(),
						decor.AverageSpeed(decor.SizeB1024(0), "% .1f"),
					),
				)

				if fdr.rangeBytes > 0 {
					bar.SetCurrent(fdr.rangeBytes)
				}

				writer = bar.ProxyWriter(file)
			} else if progress != nil {
				tracker = newProgressWriter(file, progress, fdr.guid, total)
				writer = tracker
			}

			_, copyErr := io.Copy(writer, fdr.response.Body)
			_ = fdr.response.Body.Close()
			_ = file.Close()

			if tracker != nil {
				if finalizeErr := tracker.Finalize(); finalizeErr != nil && copyErr == nil {
					copyErr = finalizeErr
				}
			}

			if copyErr != nil {
				if bar != nil {
					bar.Abort(true)
				}
				err = fmt.Errorf("download failed for %s: %w", fdr.filename, copyErr)
				return err
			}

			return nil
		})
	}

	// Wait for all downloads
	_ = eg.Wait()
	if p != nil {
		p.Wait()
	}

	// Combine errors
	var combinedError error
	mu.Lock()
	if len(allErrors) > 0 {
		combinedError = errors.Join(allErrors...)
	}
	mu.Unlock()

	downloaded := int(success.Load())

	// Print scoreboard summary
	sb.PrintSB()

	if combinedError != nil {
		logger.Info(fmt.Sprintf("%d files downloaded, but %d failed:", downloaded, len(allErrors)))
		logger.Info(combinedError.Error())
	} else {
		logger.Info(fmt.Sprintf("%d files downloaded successfully.", downloaded))
	}

	return downloaded, combinedError
}
