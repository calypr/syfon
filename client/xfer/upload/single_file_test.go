package upload

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/common"
)

func TestUploadSingleSuccessAndFailures(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	logger := &spyLogger{}
	stub := &uploaderStub{}
	file := createTempFileWithData(t, "payload")

	err := UploadSingle(ctx, stub, logger, file.Name(), "object-key", "guid-1", "bucket-a", common.FileMetadata{Metadata: map[string]any{"a": "b"}}, false)
	if err != nil {
		t.Fatalf("UploadSingle returned error: %v", err)
	}
	if len(logger.succeeded) != 1 {
		t.Fatalf("expected one success log, got %d", len(logger.succeeded))
	}
	if stub.lastUpload.url == "" || stub.lastUpload.body != "payload" || stub.lastUpload.size != int64(len("payload")) {
		t.Fatalf("unexpected upload capture: %+v", stub.lastUpload)
	}

	missingPath := file.Name() + ".missing"
	err = UploadSingle(ctx, stub, logger, missingPath, "object-key", "guid-2", "bucket-a", common.FileMetadata{}, false)
	if err == nil || !strings.Contains(err.Error(), "error opening file") {
		t.Fatalf("expected open-file error, got %v", err)
	}
	if len(logger.failed) == 0 {
		t.Fatal("expected failed log entry for open error")
	}

	resolveErrStub := &uploaderStub{resolveFunc: func(context.Context, string, string, common.FileMetadata, string) (string, error) {
		return "", errors.New("no url")
	}}
	err = UploadSingle(ctx, resolveErrStub, logger, file.Name(), "object-key", "guid-3", "bucket-a", common.FileMetadata{}, false)
	if err == nil || !strings.Contains(err.Error(), "upload error") {
		t.Fatalf("expected resolve upload error, got %v", err)
	}

	uploadErrStub := &uploaderStub{uploadFunc: func(context.Context, string, io.Reader, int64) error {
		return errors.New("upload failed")
	}}
	err = UploadSingle(ctx, uploadErrStub, logger, file.Name(), "object-key", "guid-4", "bucket-a", common.FileMetadata{}, false)
	if err == nil || !strings.Contains(err.Error(), "upload failed") {
		t.Fatalf("expected upload error, got %v", err)
	}
}

