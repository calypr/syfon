package download

import (
	"fmt"
	"io"

	"github.com/calypr/syfon/client/pkg/common"
)

type progressWriter struct {
	writer           io.Writer
	onProgress       common.ProgressCallback
	hash             string
	total            int64
	bytesSoFar       int64
	bytesSinceReport int64
}

func newProgressWriter(writer io.Writer, onProgress common.ProgressCallback, hash string, total int64) *progressWriter {
	return &progressWriter{
		writer:     writer,
		onProgress: onProgress,
		hash:       hash,
		total:      total,
	}
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.writer.Write(p)
	if n > 0 && pw.onProgress != nil {
		delta := int64(n)
		pw.bytesSoFar += delta
		pw.bytesSinceReport += delta

		if pw.bytesSinceReport >= common.OnProgressThreshold {
			if progressErr := pw.onProgress(common.ProgressEvent{
				Event:          "progress",
				Oid:            pw.hash,
				BytesSoFar:     pw.bytesSoFar,
				BytesSinceLast: pw.bytesSinceReport,
			}); progressErr != nil {
				return n, progressErr
			}
			pw.bytesSinceReport = 0
		}
	}
	return n, err
}

func (pw *progressWriter) Finalize() error {
	if pw.onProgress != nil && pw.bytesSinceReport > 0 {
		_ = pw.onProgress(common.ProgressEvent{
			Event:          "progress",
			Oid:            pw.hash,
			BytesSoFar:     pw.bytesSoFar,
			BytesSinceLast: pw.bytesSinceReport,
		})
		pw.bytesSinceReport = 0
	}
	if pw.total > 0 && pw.bytesSoFar < pw.total {
		delta := pw.total - pw.bytesSoFar
		pw.bytesSoFar = pw.total
		if pw.onProgress != nil {
			_ = pw.onProgress(common.ProgressEvent{
				Event:          "progress",
				Oid:            pw.hash,
				BytesSoFar:     pw.bytesSoFar,
				BytesSinceLast: delta,
			})
		}
		return fmt.Errorf("download incomplete: %d/%d bytes", pw.bytesSoFar-delta, pw.total)
	}
	return nil
}
