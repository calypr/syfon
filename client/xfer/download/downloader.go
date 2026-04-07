package download

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/transfer"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// DownloadMultiple is the public entry point called from g3cmd
func DownloadMultiple(
	ctx context.Context,
	dc drs.Client,
	bk transfer.Downloader,
	objects []common.ManifestObject,
	downloadPath string,
	filenameFormat string,
	rename bool,
	noPrompt bool,
	protocol string,
	numParallel int,
	skipCompleted bool,
) error {
	logger := bk.Logger()

	// === Input validation ===
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

	filenameFormat = strings.ToLower(strings.TrimSpace(filenameFormat))
	if filenameFormat != "original" && filenameFormat != "guid" && filenameFormat != "combined" {
		return fmt.Errorf("filename-format must be one of: original, guid, combined")
	}
	if (filenameFormat == "guid" || filenameFormat == "combined") && rename {
		logger.WarnContext(ctx, "NOTICE: rename flag is ignored in guid/combined mode")
		rename = false
	}

	// === Warnings and user confirmation ===
	if err := handleWarningsAndConfirmation(ctx, logger.Logger, downloadPath, filenameFormat, rename, noPrompt); err != nil {
		return err // aborted by user
	}

	// === Create download directory ===
	if err := os.MkdirAll(downloadPath, 0766); err != nil {
		return fmt.Errorf("cannot create directory %s: %w", downloadPath, err)
	}

	// === Prepare files (metadata + local validation) ===
	toDownload, skipped, renamed, err := prepareFiles(ctx, dc, bk, objects, downloadPath, filenameFormat, rename, skipCompleted, protocol)
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "Summary",
		"Total objects", len(objects),
		"To download", len(toDownload),
		"Skipped", len(skipped))

	// === Download phase ===
	downloaded, downloadErr := downloadFiles(ctx, bk, toDownload, numParallel, protocol)

	// === Final summary ===
	logger.InfoContext(ctx, fmt.Sprintf("%d files downloaded successfully.", downloaded))
	printRenamed(ctx, logger.Logger, renamed)
	printSkipped(ctx, logger.Logger, skipped)

	if downloadErr != nil {
		logger.WarnContext(ctx, "Some downloads failed. See errors above.")
	}

	return nil // we log failures but don't fail the whole command unless critical
}

// handleWarningsAndConfirmation prints warnings and asks for confirmation if needed
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

// prepareFiles gathers metadata, checks local files, collects skips/renames
func prepareFiles(
	ctx context.Context,
	dc drs.Client,
	bk transfer.Downloader,
	objects []common.ManifestObject,
	downloadPath, filenameFormat string,
	rename, skipCompleted bool,
	protocol string,
) ([]common.FileDownloadResponseObject, []RenamedOrSkippedFileInfo, []RenamedOrSkippedFileInfo, error) {
	logger := bk.Logger()
	renamed := make([]RenamedOrSkippedFileInfo, 0)
	skipped := make([]RenamedOrSkippedFileInfo, 0)
	toDownload := make([]common.FileDownloadResponseObject, 0, len(objects))

	p := mpb.New(mpb.WithOutput(os.Stdout))
	bar := p.AddBar(int64(len(objects)),
		mpb.PrependDecorators(decor.Name("Preparing "), decor.CountersNoUnit("%d / %d")),
		mpb.AppendDecorators(decor.Percentage()),
	)

	for _, obj := range objects {
		if obj.GUID == "" {
			logger.WarnContext(ctx, "Empty GUID, skipping entry")
			bar.Increment()
			continue
		}

		info := &IndexdResponse{Name: obj.Title, Size: obj.Size}
		var err error
		if info.Name == "" || info.Size == 0 {
			// Very strict object id checking
			info, err = GetFileInfo(ctx, dc, logger, obj.GUID, protocol, downloadPath, filenameFormat, rename, &renamed)
			if err != nil {
				return nil, nil, nil, err
			}
		}

		fdr := common.FileDownloadResponseObject{
			DownloadPath: downloadPath,
			Filename:     info.Name,
			GUID:         obj.GUID,
		}

		if !rename {
			validateLocalFileStat(logger.Logger, &fdr, int64(info.Size), skipCompleted)
		}

		if fdr.Skip {
			logger.InfoContext(ctx, fmt.Sprintf("Skipping %q (GUID: %s) – complete local copy exists", fdr.Filename, fdr.GUID))
			skipped = append(skipped, RenamedOrSkippedFileInfo{GUID: fdr.GUID, OldFilename: fdr.Filename})
		} else {
			toDownload = append(toDownload, fdr)
		}

		bar.Increment()
	}
	p.Wait()
	logger.InfoContext(ctx, "Preparation complete")
	return toDownload, skipped, renamed, nil
}
