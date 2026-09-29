package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

type queuedPartBackend struct {
	mu sync.Mutex

	firstRun           bool
	firstRunParts      []int
	resumeParts        []int
	allFirstRunStarted chan struct{}
	releaseActiveParts chan struct{}
	releaseOnce        sync.Once

	initCalls      int
	completedParts []transfer.MultipartPart
}

func newQueuedPartBackend() *queuedPartBackend {
	return &queuedPartBackend{
		firstRun:           true,
		allFirstRunStarted: make(chan struct{}),
		releaseActiveParts: make(chan struct{}),
	}
}

func (b *queuedPartBackend) Logger() transfer.TransferLogger { return transfer.NoOpLogger{} }

func (b *queuedPartBackend) Upload(context.Context, string, io.Reader, int64) error {
	return errors.New("single-part upload is unused")
}

func (b *queuedPartBackend) MultipartInit(context.Context, string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.initCalls++
	return "queued-part-upload", nil
}

func (b *queuedPartBackend) MultipartPart(ctx context.Context, _ string, _ string, partNum int, body io.Reader) (string, error) {
	b.mu.Lock()
	firstRun := b.firstRun
	if firstRun {
		b.firstRunParts = append(b.firstRunParts, partNum)
		if len(b.firstRunParts) == common.MaxConcurrentUploads {
			close(b.allFirstRunStarted)
		}
	} else {
		b.resumeParts = append(b.resumeParts, partNum)
	}
	b.mu.Unlock()

	if firstRun {
		if partNum == 1 {
			<-b.allFirstRunStarted
			return "", transfer.NonRetryable(errors.New("terminal part failure"))
		}
		select {
		case <-ctx.Done():
		case <-b.releaseActiveParts:
		}
	}

	if _, err := io.Copy(io.Discard, body); err != nil {
		return "", err
	}
	return fmt.Sprintf("etag-%d", partNum), nil
}

func (b *queuedPartBackend) MultipartComplete(_ context.Context, _ string, _ string, parts []transfer.MultipartPart) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.completedParts = append([]transfer.MultipartPart(nil), parts...)
	return nil
}

func (b *queuedPartBackend) releaseParts() {
	b.releaseOnce.Do(func() { close(b.releaseActiveParts) })
}

func (b *queuedPartBackend) firstRunPartNumbers() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.firstRunParts...)
}

func (b *queuedPartBackend) resumedPartNumbers() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.resumeParts...)
}

func TestMultipartPartFailureStopsQueuedWorkAndResumes(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	sourcePath := filepath.Join(t.TempDir(), "source.bin")
	const fileSize = 101 * common.MB
	source, err := os.Create(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Truncate(fileSize); err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	backend := newQueuedPartBackend()
	uploader := &GenericUploader{Backend: backend}
	request := transfer.TransferRequest{SourcePath: sourcePath, GUID: "queued-part-object", ForceMultipart: true}
	uploadDone := make(chan error, 1)
	go func() { uploadDone <- uploader.Upload(context.Background(), request) }()

	select {
	case <-backend.allFirstRunStarted:
	case <-time.After(5 * time.Second):
		backend.releaseParts()
		t.Fatal("multipart upload did not start all ten active parts")
	}

	var uploadErr error
	select {
	case uploadErr = <-uploadDone:
	case <-time.After(time.Second):
		backend.releaseParts()
		uploadErr = <-uploadDone
	}
	backend.releaseParts()
	if uploadErr == nil || !strings.Contains(uploadErr.Error(), "terminal part failure") {
		t.Fatalf("first multipart upload error = %v, want terminal part failure", uploadErr)
	}

	firstRunParts := backend.firstRunPartNumbers()
	for partNum := 1; partNum <= common.MaxConcurrentUploads; partNum++ {
		found := false
		for _, got := range firstRunParts {
			if got == partNum {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("initial active part %d was not started: %v", partNum, firstRunParts)
		}
	}
	for _, partNum := range firstRunParts {
		if partNum == common.MaxConcurrentUploads+1 {
			t.Fatalf("queued part %d started after terminal failure: %v", partNum, firstRunParts)
		}
	}

	checkpointPath, err := CheckpointPath(sourcePath, request.GUID)
	if err != nil {
		t.Fatal(err)
	}
	state, ok := uploader.loadState(checkpointPath)
	if !ok || state.Phase != uploadCheckpointUploading || state.UploadID != "queued-part-upload" {
		t.Fatalf("failed upload checkpoint = %+v (present=%v), want uploading checkpoint for existing upload", state, ok)
	}
	if len(state.Completed) != common.MaxConcurrentUploads-1 {
		t.Fatalf("checkpoint completed parts = %v, want the nine successful active parts", state.Completed)
	}

	backend.mu.Lock()
	backend.firstRun = false
	backend.mu.Unlock()
	if err := uploader.Upload(context.Background(), request); err != nil {
		t.Fatalf("explicit resume returned error: %v", err)
	}
	if backend.initCalls != 1 {
		t.Fatalf("multipart init calls = %d, want existing upload reused on resume", backend.initCalls)
	}
	resumed := backend.resumedPartNumbers()
	if len(resumed) != 2 || (resumed[0] != 1 && resumed[1] != 1) || (resumed[0] != common.MaxConcurrentUploads+1 && resumed[1] != common.MaxConcurrentUploads+1) {
		t.Fatalf("resumed part numbers = %v, want failed part 1 and queued part %d", resumed, common.MaxConcurrentUploads+1)
	}
	if len(backend.completedParts) != common.MaxConcurrentUploads+1 {
		t.Fatalf("completion parts = %v, want all eleven parts", backend.completedParts)
	}
	if _, err := os.Stat(checkpointPath); !os.IsNotExist(err) {
		t.Fatalf("checkpoint after successful resume stat error = %v, want removed", err)
	}
}
