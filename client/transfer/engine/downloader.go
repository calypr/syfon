package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
	"golang.org/x/sync/errgroup"
)

type GenericDownloader struct {
	Backend transfer.Backend
}

func (d *GenericDownloader) Download(ctx context.Context, guid string, dstPath string, concurrency int, chunkSize, multipartThreshold int64) error {
	meta, err := d.Backend.Stat(ctx, guid)
	if err != nil {
		return fmt.Errorf("stat failed: %w", err)
	}

	totalSize := meta.Size
	if totalSize <= 0 {
		return d.downloadSingle(ctx, guid, dstPath, totalSize)
	}

	if multipartThreshold > 0 && totalSize < multipartThreshold {
		return d.downloadSingle(ctx, guid, dstPath, totalSize)
	}

	if totalSize < common.MB || !meta.AcceptRanges {
		return d.downloadSingle(ctx, guid, dstPath, totalSize)
	}

	return d.downloadParallel(ctx, guid, dstPath, totalSize, concurrency, chunkSize)
}

func (d *GenericDownloader) downloadSingle(ctx context.Context, guid string, dstPath string, expectedSize int64) error {
	var startOffset int64
	if stat, err := os.Stat(dstPath); err == nil {
		if expectedSize > 0 && stat.Size() == expectedSize {
			return nil // Already complete
		}
		if stat.Size() > 0 && expectedSize > stat.Size() {
			startOffset = stat.Size()
		}
	}

	var body io.ReadCloser
	var err error
	if startOffset > 0 {
		body, err = d.Backend.GetRangeReader(ctx, guid, startOffset, expectedSize-startOffset)
		if err == transfer.ErrRangeIgnored {
			// Server ignored our range request, restart from zero.
			startOffset = 0
			body, err = d.Backend.GetReader(ctx, guid)
		}
	} else {
		body, err = d.Backend.GetReader(ctx, guid)
	}
	if err != nil {
		return err
	}
	defer body.Close()

	if dir := filepath.Dir(dstPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	mode := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	if startOffset > 0 {
		mode = os.O_WRONLY | os.O_APPEND
	}

	file, err := os.OpenFile(dstPath, mode, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	written, err := io.Copy(file, body)
	remaining := expectedSize - startOffset
	if err != nil {
		if progress := common.GetProgress(ctx); progress != nil && remaining > written {
			_ = progress(common.ProgressEvent{
				Event:          "progress",
				Oid:            common.GetOid(ctx),
				BytesSinceLast: remaining - written,
				BytesSoFar:     expectedSize,
			})
		}
		return err
	}

	if progress := common.GetProgress(ctx); progress != nil && written > 0 {
		_ = progress(common.ProgressEvent{
			Event:          "progress",
			Oid:            common.GetOid(ctx),
			BytesSinceLast: written,
			BytesSoFar:     startOffset + written,
		})
	}
	if expectedSize > 0 && (startOffset+written) < expectedSize {
		if progress := common.GetProgress(ctx); progress != nil {
			_ = progress(common.ProgressEvent{
				Event:          "progress",
				Oid:            common.GetOid(ctx),
				BytesSinceLast: expectedSize - (startOffset + written),
				BytesSoFar:     expectedSize,
			})
		}
		return fmt.Errorf("short download: got %d, expected %d", startOffset+written, expectedSize)
	}
	emitTransferCompletion(ctx, common.TransferCompletionEvent{
		Direction:  "download",
		GUID:       guid,
		RangeStart: startOffset,
		RangeEnd:   startOffset + written - 1,
		Bytes:      written,
		Strategy:   "single",
	})
	return nil
}

func (d *GenericDownloader) downloadParallel(ctx context.Context, guid string, dstPath string, totalSize int64, concurrency int, chunkSize int64) error {
	if dir := filepath.Dir(dstPath); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	file, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(totalSize); err != nil {
		return fmt.Errorf("pre-allocate failed: %w", err)
	}

	if chunkSize <= 0 {
		chunkSize = 64 * common.MB
	}
	if concurrency <= 0 {
		concurrency = 8
	}

	totalParts := int((totalSize + chunkSize - 1) / chunkSize)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	var soFar atomic.Int64
	bufPool := sync.Pool{
		New: func() any { return make([]byte, 256*1024) },
	}

	progress := common.GetProgress(ctx)
	oid := common.GetOid(ctx)

	for i := 0; i < totalParts; i++ {
		ps := int64(i) * chunkSize
		pe := ps + chunkSize - 1
		if pe >= totalSize {
			pe = totalSize - 1
		}
		partSize := pe - ps + 1
		partStart := ps
		partEnd := pe
		partLength := partSize

		g.Go(func() error {
			strategy := transfer.DefaultBackoff()
			return transfer.RetryAction(gctx, d.Backend.Logger(), strategy, common.MaxRetryCount, func() error {
				partBody, err := d.Backend.GetRangeReader(gctx, guid, partStart, partLength)
				if err != nil {
					return fmt.Errorf("range download [%d,%d]: %w", partStart, partEnd, err)
				}
				defer partBody.Close()

				w := io.NewOffsetWriter(file, partStart)
				buf := bufPool.Get().([]byte)
				written, err := io.CopyBuffer(w, partBody, buf)
				bufPool.Put(buf)
				if err != nil {
					return err
				}
				if written != partLength {
					return fmt.Errorf("short write: got %d, expected %d", written, partLength)
				}

				if progress != nil {
					current := soFar.Add(written)
					_ = progress(common.ProgressEvent{
						Event:          "progress",
						Oid:            oid,
						BytesSinceLast: written,
						BytesSoFar:     current,
					})
				}
				emitTransferCompletion(gctx, common.TransferCompletionEvent{
					Direction:  "download",
					GUID:       guid,
					RangeStart: partStart,
					RangeEnd:   partEnd,
					Bytes:      written,
					PartNumber: partNum(partStart, chunkSize),
					Strategy:   "multipart",
				})
				return nil
			})
		})
	}

	if err := g.Wait(); err != nil {
		// Parallel downloads pre-allocate the destination file to its final size.
		// If any part fails, remove the incomplete file so retries do not mistake it
		// for a completed cache entry.
		_ = file.Close()
		_ = os.Remove(dstPath)
		return err
	}

	return nil
}

func emitTransferCompletion(ctx context.Context, ev common.TransferCompletionEvent) {
	if ev.Bytes <= 0 {
		return
	}
	if cb := common.GetTransferCompletion(ctx); cb != nil {
		_ = cb(ev)
	}
}

func partNum(start, chunkSize int64) int {
	if chunkSize <= 0 {
		return 0
	}
	return int(start/chunkSize) + 1
}
