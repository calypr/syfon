package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/calypr/syfon/client/drs"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/xfer"
	"golang.org/x/sync/errgroup"
)

type DownloadOptions struct {
	MultipartThreshold int64
	ChunkSize          int64
	Concurrency        int
}

func defaultDownloadOptions() DownloadOptions {
	return DownloadOptions{
		MultipartThreshold: common.GB,
		ChunkSize:          64 * common.MB,
		Concurrency:        8,
	}
}

// DownloadSingleWithProgress downloads a single object while emitting progress events.
func DownloadSingleWithProgress(
	ctx context.Context,
	dc drs.Client,
	bk xfer.Downloader,
	guid string,
	downloadPath string,
	protocol string,
) error {
	progress := common.GetProgress(ctx)
	var err error
	downloadPath, err = common.ParseRootPath(downloadPath)
	if err != nil {
		return fmt.Errorf("invalid download path: %w", err)
	}
	if !strings.HasSuffix(downloadPath, "/") {
		downloadPath += "/"
	}

	renamed := make([]RenamedOrSkippedFileInfo, 0)
	info, err := GetFileInfo(ctx, dc, bk.Logger(), guid, protocol, downloadPath, "original", false, &renamed)
	if err != nil {
		return err
	}

	fdr := downloadRequest{downloadPath: downloadPath, filename: info.Name, guid: guid}

	if err := GetDownloadResponse(ctx, bk, &fdr, protocol); err != nil {
		return err
	}

	fullPath := filepath.Join(fdr.downloadPath, fdr.filename)
	if dir := filepath.Dir(fullPath); dir != "." {
		if err = os.MkdirAll(dir, 0766); err != nil {
			_ = fdr.response.Body.Close()
			return fmt.Errorf("mkdir for %s: %w", fullPath, err)
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
		return fmt.Errorf("open local file %s: %w", fullPath, err)
	}

	total := info.Size
	var writer io.Writer = file
	var tracker *progressWriter
	if progress != nil {
		tracker = newProgressWriter(file, progress, guid, total)
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
		return fmt.Errorf("download failed for %s: %w", fdr.filename, copyErr)
	}
	return nil
}

// DownloadToPath downloads a single object using the provided backend
func DownloadToPath(
	ctx context.Context,
	dc drs.Client,
	bk xfer.Downloader,
	logger *slog.Logger,
	guid string,
	dstPath string,
	protocol string,
) error {
	opts := defaultDownloadOptions()
	return DownloadToPathWithOptions(ctx, dc, bk, logger, guid, dstPath, protocol, opts)
}

func DownloadToPathWithOptions(
	ctx context.Context,
	dc drs.Client,
	bk xfer.Downloader,
	logger *slog.Logger,
	guid string,
	dstPath string,
	protocol string,
	opts DownloadOptions,
) error {
	if opts.MultipartThreshold <= 0 {
		opts.MultipartThreshold = defaultDownloadOptions().MultipartThreshold
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = defaultDownloadOptions().ChunkSize
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultDownloadOptions().Concurrency
	}

	info, err := drs.ResolveObject(ctx, dc, guid)
	if err != nil {
		return fmt.Errorf("get file details failed: %w", err)
	}

	// If size is unknown or small, single stream is safest.
	if info.Size <= 0 || info.Size < opts.MultipartThreshold {
		return downloadToPathSingle(ctx, bk, logger, guid, dstPath, protocol, info.Size)
	}

	// If a partial file already exists, resumable single-stream download is safer than
	// parallel range writes and avoids restarting from zero.
	if st, statErr := os.Stat(dstPath); statErr == nil {
		if st.Size() == info.Size {
			return nil
		}
		if st.Size() > 0 && st.Size() < info.Size {
			return downloadToPathSingle(ctx, bk, logger, guid, dstPath, protocol, info.Size)
		}
	}

	if err := downloadToPathMultipart(ctx, bk, logger, guid, dstPath, protocol, info.Size, opts); err != nil {
		return err
	}

	return nil
}

func downloadToPathSingle(
	ctx context.Context,
	bk xfer.Downloader,
	logger *slog.Logger,
	guid string,
	dstPath string,
	protocol string,
	expectedSize int64,
) error {
	progress := common.GetProgress(ctx)
	hash := common.GetOid(ctx)

	var existingSize int64
	if st, err := os.Stat(dstPath); err == nil {
		existingSize = st.Size()
		if expectedSize > 0 && existingSize == expectedSize {
			return nil
		}
	}

	fdr := downloadRequest{guid: guid}
	if existingSize > 0 {
		fdr.rangeBytes = existingSize
		fdr.rangeStart = &fdr.rangeBytes
	}

	if err := GetDownloadResponse(ctx, bk, &fdr, protocol); err != nil {
		// Mimic failed context logging from original
		// We'd need to reconstruct the "logger.FailedContext" logic if using raw slog
		// For now, simple error logging or rely on caller to log context?
		// The original code used g3i.Logger().FailedContext...
		// Let's just log error
		logger.Error("Download failed", "error", err, "path", dstPath, "guid", guid)
		return err
	}
	defer fdr.response.Body.Close()

	if existingSize > 0 && fdr.response.StatusCode == http.StatusOK {
		// Server ignored range; restart from zero.
		existingSize = 0
	}

	if dir := filepath.Dir(dstPath); dir != "." {
		if err := os.MkdirAll(dir, 0766); err != nil {
			logger.Error("Mkdir failed", "error", err, "path", dstPath)
			return fmt.Errorf("mkdir for %s: %w", dstPath, err)
		}
	}

	flags := os.O_CREATE | os.O_WRONLY
	if existingSize > 0 {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(dstPath, flags, 0666)
	if err != nil {
		logger.Error("Create file failed", "error", err, "path", dstPath)
		return fmt.Errorf("create local file %s: %w", dstPath, err)
	}
	defer file.Close()

	var writer io.Writer = file
	if progress != nil {
		total := fdr.response.ContentLength + existingSize
		tracker := newProgressWriter(file, progress, hash, total)
		if existingSize > 0 {
			tracker.bytesSoFar = existingSize
		}
		writer = tracker
		defer tracker.Finalize()
	}

	reader := io.Reader(fdr.response.Body)
	if failAfter := parseInjectedDownloadFailureBytes(); failAfter > 0 {
		reader = &failAfterReader{
			r:         reader,
			remaining: failAfter,
		}
	}

	if _, err := io.Copy(writer, reader); err != nil {
		logger.Error("Copy failed", "error", err, "path", dstPath)
		return fmt.Errorf("copy to %s: %w", dstPath, err)
	}
	if expectedSize > 0 {
		if st, err := os.Stat(dstPath); err == nil && st.Size() != expectedSize {
			return fmt.Errorf("download incomplete for %s: expected %d bytes, got %d", dstPath, expectedSize, st.Size())
		}
	}

	// Success logging is up to caller or we can do simple info
	// logger.Info("Download succeeded", "path", dstPath, "guid", guid)
	return nil
}

func parseInjectedDownloadFailureBytes() int64 {
	raw := strings.TrimSpace(os.Getenv("DATA_CLIENT_TEST_FAIL_DOWNLOAD_AFTER_BYTES"))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

type failAfterReader struct {
	r         io.Reader
	remaining int64
	failed    bool
}

func (f *failAfterReader) Read(p []byte) (int, error) {
	if f.failed {
		return 0, errors.New("injected test interruption during download")
	}
	if f.remaining <= 0 {
		f.failed = true
		return 0, errors.New("injected test interruption during download")
	}
	if int64(len(p)) > f.remaining {
		p = p[:f.remaining]
	}
	n, err := f.r.Read(p)
	f.remaining -= int64(n)
	if err != nil {
		return n, err
	}
	if f.remaining <= 0 {
		f.failed = true
		return n, errors.New("injected test interruption during download")
	}
	return n, nil
}

func downloadToPathMultipart(
	ctx context.Context,
	bk xfer.Downloader,
	logger *slog.Logger,
	guid string,
	dstPath string,
	protocol string,
	totalSize int64,
	opts DownloadOptions,
) error {
	signedURL, err := bk.ResolveDownloadURL(ctx, guid, protocol)
	if err != nil {
		return fmt.Errorf("failed to resolve download URL for %s: %w", guid, err)
	}

	// Preflight first ranged read to verify server honors ranges.
	rangeStart := int64(0)
	rangeEnd := opts.ChunkSize - 1
	if rangeEnd >= totalSize {
		rangeEnd = totalSize - 1
	}
	resp, err := bk.Download(ctx, signedURL, &rangeStart, &rangeEnd)
	if err != nil {
		return fmt.Errorf("multipart preflight request failed: %w", err)
	}
	if resp.StatusCode != 206 {
		bodyErr := common.ResponseBodyError(resp, "range requests not supported")
		_ = resp.Body.Close()
		return bodyErr
	}
	_ = resp.Body.Close()

	if dir := filepath.Dir(dstPath); dir != "." {
		if err := os.MkdirAll(dir, 0766); err != nil {
			return fmt.Errorf("mkdir for %s: %w", dstPath, err)
		}
	}

	file, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("create local file %s: %w", dstPath, err)
	}
	defer file.Close()

	if err := file.Truncate(totalSize); err != nil {
		return fmt.Errorf("pre-allocate %s: %w", dstPath, err)
	}

	progress := common.GetProgress(ctx)
	hash := common.GetOid(ctx)
	if hash == "" {
		hash = guid
	}
	var soFar atomic.Int64
	bufPool := sync.Pool{
		New: func() any {
			return make([]byte, 256*1024)
		},
	}

	totalParts := int((totalSize + opts.ChunkSize - 1) / opts.ChunkSize)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	for i := 0; i < totalParts; i++ {
		partStart := int64(i) * opts.ChunkSize
		partEnd := partStart + opts.ChunkSize - 1
		if partEnd >= totalSize {
			partEnd = totalSize - 1
		}
		ps := partStart
		pe := partEnd

		g.Go(func() error {
			partResp, err := bk.Download(gctx, signedURL, &ps, &pe)
			if err != nil {
				return fmt.Errorf("range download %d-%d failed: %w", ps, pe, err)
			}

			if partResp.StatusCode != 206 {
				bodyErr := common.ResponseBodyError(partResp, fmt.Sprintf("range download %d-%d returned", ps, pe))
				_ = partResp.Body.Close()
				return bodyErr
			}
			defer partResp.Body.Close()

			partSize := pe - ps + 1
			w := io.NewOffsetWriter(file, ps)
			buf := bufPool.Get().([]byte)
			written, err := io.CopyBuffer(w, io.LimitReader(partResp.Body, partSize), buf)
			bufPool.Put(buf)
			if err != nil {
				return fmt.Errorf("range copy %d-%d failed after %d/%d bytes: %w", ps, pe, written, partSize, err)
			}
			if written != partSize {
				return fmt.Errorf("range copy %d-%d was short: got %d/%d bytes", ps, pe, written, partSize)
			}

			if progress != nil {
				current := soFar.Add(written)
				_ = progress(common.ProgressEvent{
					Event:          "progress",
					Oid:            hash,
					BytesSinceLast: written,
					BytesSoFar:     current,
				})
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if progress != nil {
		final := soFar.Load()
		if final < totalSize {
			_ = progress(common.ProgressEvent{
				Event:          "progress",
				Oid:            hash,
				BytesSinceLast: totalSize - final,
				BytesSoFar:     totalSize,
			})
		}
	}

	logger.Info("multipart download completed", "guid", guid, "size", totalSize)
	return nil
}
