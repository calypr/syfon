package download

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func DownloadMultiple(
	ctx context.Context,
	dc drs.Client,
	bk xfer.Downloader,
	guids []string,
	downloadPath string,
	filenameFormat string,
	rename bool,
	noPrompt bool,
	protocol string,
	numParallel int,
	skipCompleted bool,
) error {
	logger := bk.Logger()
	if numParallel < 1 {
		return fmt.Errorf("numparallel must be a positive integer")
	}
	var err error
	downloadPath, err = common.ParseRootPath(downloadPath)
	if err != nil {
		return fmt.Errorf("invalid download path: %w", err)
	}
	if !strings.HasSuffix(downloadPath, "/") {
		downloadPath += "/"
	}
	if err := handleWarningsAndConfirmation(ctx, logger.Slog(), downloadPath, filenameFormat, rename, noPrompt); err != nil {
		return err
	}
	if err := os.MkdirAll(downloadPath, 0766); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", downloadPath, err)
	}
	toDownload, skipped, renamed, err := prepareFiles(ctx, dc, bk, guids, downloadPath, filenameFormat, rename, skipCompleted, protocol)
	if err != nil {
		return err
	}
	downloaded, downloadErr := downloadFiles(ctx, bk, toDownload, numParallel, protocol)
	logger.InfoContext(ctx, fmt.Sprintf("%d files downloaded successfully.", downloaded))
	printRenamed(ctx, logger.Slog(), renamed)
	printSkipped(ctx, logger.Slog(), skipped)
	if downloadErr != nil {
		logger.WarnContext(ctx, "Some downloads failed. See errors above.")
	}
	return nil
}

func handleWarningsAndConfirmation(ctx context.Context, logger *slog.Logger, downloadPath, filenameFormat string, rename, noPrompt bool) error {
	if filenameFormat == "guid" || filenameFormat == "combined" {
		logger.WarnContext(ctx, fmt.Sprintf("WARNING: in %q mode, duplicate files in %q will be overwritten", filenameFormat, downloadPath))
	} else if !rename {
		logger.WarnContext(ctx, fmt.Sprintf("WARNING: rename=false in original mode – duplicates in %q will be overwritten", downloadPath))
	} else {
		logger.InfoContext(ctx, fmt.Sprintf("NOTICE: rename=true in original mode – duplicates in %q will be renamed with a counter", downloadPath))
	}
	if noPrompt {
		return nil
	}
	if !AskForConfirmation(logger, "Proceed? (y/N)") {
		return fmt.Errorf("aborted by user")
	}
	return nil
}

func prepareFiles(
	ctx context.Context,
	dc drs.Client,
	bk xfer.Downloader,
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
