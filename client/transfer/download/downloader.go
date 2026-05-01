package download

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/calypr/syfon/client/transfer/engine"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"
)

type Resolver interface {
	Resolve(ctx context.Context, id string) (*transfer.ResolvedObject, error)
}

func DownloadMultiple(
	ctx context.Context,
	dc Resolver,
	bk transfer.ReadBackend,
	guids []string,
	downloadPath string,
	numParallel int,
	skipCompleted bool,
) error {
	logger := bk.Logger()
	if numParallel < 1 {
		numParallel = 1
	}

	downloadPath, err := common.ParseRootPath(downloadPath)
	if err != nil {
		return fmt.Errorf("invalid download path: %w", err)
	}
	if !strings.HasSuffix(downloadPath, "/") {
		downloadPath += "/"
	}

	if err := os.MkdirAll(downloadPath, 0766); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", downloadPath, err)
	}

	toDownload, skipped, renamed, err := prepareFiles(ctx, dc, bk, guids, downloadPath, "original", true, skipCompleted, "https")
	if err != nil {
		return err
	}

	if len(toDownload) == 0 {
		logger.Info("No files to download.")
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(numParallel)

	downloader := &engine.GenericDownloader{Source: bk}
	var success atomic.Int64
	var mu sync.Mutex
	var allErrors []error

	for _, fdr := range toDownload {
		fdr := fdr // capture
		g.Go(func() error {
			dst := filepath.Join(fdr.downloadPath, fdr.filename)
			// Using 64MB chunks for parallel download by default
			if err := downloader.Download(gctx, fdr.guid, dst, 4, 64*common.MB, int64(5*common.GB)); err != nil {
				mu.Lock()
				allErrors = append(allErrors, fmt.Errorf("guid %s: %w", fdr.guid, err))
				mu.Unlock()
				return err
			}
			success.Add(1)
			return nil
		})
	}

	_ = g.Wait()

	printRenamed(ctx, logger.Slog(), renamed)
	printSkipped(ctx, logger.Slog(), skipped)

	if len(allErrors) > 0 {
		return fmt.Errorf("some downloads failed: %v", allErrors)
	}

	return nil
}

func prepareFiles(
	ctx context.Context,
	dc Resolver,
	bk transfer.ReadBackend,
	guids []string,
	downloadPath, filenameFormat string,
	rename, skipCompleted bool,
	protocol string,
) ([]downloadRequest, []RenamedOrSkippedFileInfo, []RenamedOrSkippedFileInfo, error) {
	logger := bk.Logger()
	renamed := make([]RenamedOrSkippedFileInfo, 0)
	skipped := make([]RenamedOrSkippedFileInfo, 0)
	toDownload := make([]downloadRequest, 0, len(guids))

	p := mpb.New(mpb.WithOutput(os.Stdout))
	bar := p.AddBar(int64(len(guids)), mpb.PrependDecorators(decor.Name("Preparing "), decor.CountersNoUnit("%d / %d")), mpb.AppendDecorators(decor.Percentage()))

	for _, guid := range guids {
		if guid == "" {
			bar.Increment()
			continue
		}
		info, err := GetFileInfo(ctx, dc, logger, guid, protocol, downloadPath, filenameFormat, rename, &renamed)
		if err != nil {
			p.Wait()
			return nil, nil, nil, err
		}
		fdr := downloadRequest{downloadPath: downloadPath, filename: info.Name, guid: guid}
		if !rename {
			validateLocalFileStat(logger.Slog(), &fdr, int64(info.Size), skipCompleted)
		}
		if fdr.skip {
			skipped = append(skipped, RenamedOrSkippedFileInfo{GUID: fdr.guid, OldFilename: fdr.filename})
		} else {
			toDownload = append(toDownload, fdr)
		}
		bar.Increment()
	}
	p.Wait()
	return toDownload, skipped, renamed, nil
}
