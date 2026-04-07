package transfer

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/calypr/syfon/client/pkg/common"
)

// UploadSingle is the execution engine for single-stream uploads.
// Consumes the ObjectWriter capability interface.
func UploadSingle(
	ctx context.Context,
	writer ObjectWriter,
	guid string,
	key string,
	file *os.File,
	showProgress bool,
) error {
	fi, _ := file.Stat()
	fileSize := fi.Size()

	w, err := writer.GetWriter(ctx, guid)
	if err != nil {
		return fmt.Errorf("failed to open cloud writer: %w", err)
	}
	defer w.Close()

	progressCallback := common.GetProgress(ctx)
	oid := common.GetOid(ctx)
	if oid == "" {
		oid = guid
	}

	var r io.Reader = file
	if progressCallback != nil {
		pr := newProgressReader(file, progressCallback, oid, fileSize)
		r = pr
		defer pr.Finalize()
	}

	if _, err = io.Copy(w, r); err != nil {
		return fmt.Errorf("single-part upload failed: %w", err)
	}

	return nil
}
