package engine

import (
	"fmt"
	"io"
	"sync/atomic"

	"github.com/calypr/syfon/client/common"
)

type progressReader struct {
	reader           io.Reader
	onProgress       common.ProgressCallback
	oid              string
	total            int64
	localBytes       int64
	bytesSinceReport int64
	globalBytes      *int64
}

func newProgressReader(reader io.Reader, onProgress common.ProgressCallback, oid string, total int64, globalBytes *int64) *progressReader {
	return &progressReader{
		reader:      reader,
		onProgress:  onProgress,
		oid:         oid,
		total:       total,
		globalBytes: globalBytes,
	}
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 && pr.onProgress != nil {
		delta := int64(n)
		pr.bytesSinceReport += delta
		bytesSoFar := pr.advance(delta)

		if pr.bytesSinceReport >= common.OnProgressThreshold {
			if progressErr := pr.onProgress(common.ProgressEvent{
				Event:          "progress",
				Oid:            pr.oid,
				BytesSoFar:     bytesSoFar,
				BytesSinceLast: pr.bytesSinceReport,
			}); progressErr != nil {
				return n, progressErr
			}
			pr.bytesSinceReport = 0
		}
	}
	return n, err
}

func (pr *progressReader) Finalize() error {
	if pr.onProgress != nil && pr.bytesSinceReport > 0 {
		_ = pr.onProgress(common.ProgressEvent{
			Event:          "progress",
			Oid:            pr.oid,
			BytesSoFar:     pr.current(),
			BytesSinceLast: pr.bytesSinceReport,
		})
		pr.bytesSinceReport = 0
	}
	if pr.globalBytes == nil && pr.total > 0 && pr.current() < pr.total {
		return fmt.Errorf("upload incomplete: %d/%d bytes", pr.current(), pr.total)
	}
	return nil
}

func (pr *progressReader) advance(delta int64) int64 {
	if pr.globalBytes != nil {
		return atomic.AddInt64(pr.globalBytes, delta)
	}
	pr.localBytes += delta
	return pr.localBytes
}

func (pr *progressReader) current() int64 {
	if pr.globalBytes != nil {
		return atomic.LoadInt64(pr.globalBytes)
	}
	return pr.localBytes
}
