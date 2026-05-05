package download

import (
	"io"
	"log/slog"
	"net/http"
	"os"
)

type IndexdResponse struct {
	Name string
	Size int64
}
type RenamedOrSkippedFileInfo struct {
	GUID        string
	OldFilename string
	NewFilename string
}

type downloadRequest struct {
	downloadPath string
	filename     string
	guid         string
	presignedURL string
	rangeBytes   int64
	rangeStart   *int64
	rangeEnd     *int64
	overwrite    bool
	skip         bool
	response     *http.Response
	writer       io.Writer
}

func validateLocalFileStat(
	logger *slog.Logger,
	fdr *downloadRequest,
	filesize int64,
	skipCompleted bool,
) {
	fullPath := fdr.downloadPath + fdr.filename

	fi, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No local file → full download, nothing special
			return
		}
		logger.Error("Error statting local file", "path", fullPath, "error", err)
		logger.Info("Will attempt full download anyway")
		return
	}

	localSize := fi.Size()

	// User doesn't want to skip completed files → force full overwrite
	if !skipCompleted {
		fdr.overwrite = true
		return
	}

	// Exact match → skip entirely
	if localSize == filesize {
		fdr.skip = true
		return
	}

	// Local file larger than expected → overwrite fully (corrupted or different file)
	if localSize > filesize {
		fdr.overwrite = true
		return
	}

	fdr.rangeBytes = localSize
}
