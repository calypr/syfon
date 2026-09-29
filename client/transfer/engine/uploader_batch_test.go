package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/transfer"
)

type multipartWindowBackend struct {
	*fakeBackend

	mu                    sync.Mutex
	requests              [][]int32
	directAttempts        map[int]int
	fallbackCalls         int
	fallbackActive        int
	legacyResponse        bool
	unknownBatchExpiry    bool
	partExpiry            time.Time
	failDirectPart        int
	waitForActiveOnSecond bool
	cancelSecondSign      bool
	releaseSecondSign     <-chan struct{}
	releaseUploads        <-chan struct{}
	activePut             chan struct{}
	fallbackStarted       chan struct{}
	overlapObserved       chan struct{}
	signCanceled          chan struct{}
	activeOnce            sync.Once
	fallbackOnce          sync.Once
	overlapOnce           sync.Once
	cancelOnce            sync.Once
}

func newMultipartWindowBackend() *multipartWindowBackend {
	return &multipartWindowBackend{
		fakeBackend:     &fakeBackend{multipartInitID: "window-upload"},
		directAttempts:  make(map[int]int),
		activePut:       make(chan struct{}),
		fallbackStarted: make(chan struct{}),
		overlapObserved: make(chan struct{}),
		signCanceled:    make(chan struct{}),
	}
}

func (b *multipartWindowBackend) MultipartPartURLs(ctx context.Context, _ string, _ string, numbers []int32) (transfer.MultipartPartURLBatch, error) {
	b.mu.Lock()
	request := append([]int32(nil), numbers...)
	b.requests = append(b.requests, request)
	callNumber := len(b.requests)
	legacy := b.legacyResponse
	b.mu.Unlock()

	if callNumber == 2 && b.waitForActiveOnSecond {
		select {
		case <-b.activePut:
		case <-ctx.Done():
			return transfer.MultipartPartURLBatch{}, ctx.Err()
		}
		b.overlapOnce.Do(func() { close(b.overlapObserved) })
		if b.cancelSecondSign {
			<-ctx.Done()
			b.cancelOnce.Do(func() { close(b.signCanceled) })
			return transfer.MultipartPartURLBatch{}, ctx.Err()
		}
		if b.releaseSecondSign != nil {
			select {
			case <-b.releaseSecondSign:
			case <-ctx.Done():
				return transfer.MultipartPartURLBatch{}, ctx.Err()
			}
		}
	}

	if legacy {
		return transfer.MultipartPartURLBatch{
			Parts:          []transfer.MultipartPartURL{{PartNumber: numbers[0], URL: fmt.Sprintf("url-part-%d", numbers[0])}},
			BatchSupported: false,
		}, nil
	}
	if b.unknownBatchExpiry {
		return transfer.MultipartPartURLBatch{
			Parts:          []transfer.MultipartPartURL{{PartNumber: numbers[0], URL: fmt.Sprintf("url-part-%d", numbers[0]), ExpiresAt: time.Now().Add(-time.Second)}},
			BatchSupported: false,
		}, nil
	}
	expiresAt := b.partExpiry
	if expiresAt.IsZero() {
		expiresAt = time.Now().Add(time.Hour)
	}
	parts := make([]transfer.MultipartPartURL, len(numbers))
	for i, number := range numbers {
		parts[i] = transfer.MultipartPartURL{PartNumber: number, URL: fmt.Sprintf("url-part-%d", number), ExpiresAt: expiresAt}
	}
	return transfer.MultipartPartURLBatch{Parts: parts, BatchSupported: true}, nil
}

func (b *multipartWindowBackend) UploadPart(ctx context.Context, url string, body io.Reader, _ int64) (string, error) {
	partNumber := 0
	if _, err := fmt.Sscanf(url, "url-part-%d", &partNumber); err != nil {
		return "", err
	}
	b.mu.Lock()
	b.directAttempts[partNumber]++
	attempt := b.directAttempts[partNumber]
	b.mu.Unlock()
	b.activeOnce.Do(func() { close(b.activePut) })
	if partNumber == b.failDirectPart && attempt == 1 {
		return "", errors.New("stale signed URL")
	}
	if b.releaseUploads != nil {
		select {
		case <-b.releaseUploads:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		return "", err
	}
	return fmt.Sprintf("etag-%d", partNumber), nil
}

func (b *multipartWindowBackend) MultipartPart(ctx context.Context, _ string, _ string, partNumber int, body io.Reader) (string, error) {
	b.mu.Lock()
	b.fallbackCalls++
	b.fallbackActive++
	active := b.fallbackActive
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.fallbackActive--
		b.mu.Unlock()
	}()
	if active >= common.MaxConcurrentUploads-1 {
		b.fallbackOnce.Do(func() { close(b.fallbackStarted) })
	}
	if b.releaseUploads != nil {
		select {
		case <-b.releaseUploads:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		return "", err
	}
	return fmt.Sprintf("etag-%d", partNumber), nil
}

func (b *multipartWindowBackend) partRequests() [][]int32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	requests := make([][]int32, len(b.requests))
	for i := range b.requests {
		requests[i] = append([]int32(nil), b.requests[i]...)
	}
	return requests
}

func (b *multipartWindowBackend) fallbackCallCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fallbackCalls
}

func createSparseMultipartSource(t *testing.T, size int64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.bin")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(size); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMultipartSigningWindowOverlapsPartUploads(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 170*common.MB)
	releaseUploads := make(chan struct{})
	releaseSecondSign := make(chan struct{})
	backend := newMultipartWindowBackend()
	backend.waitForActiveOnSecond = true
	backend.releaseUploads = releaseUploads
	backend.releaseSecondSign = releaseSecondSign
	uploader := &GenericUploader{Backend: backend}
	done := make(chan error, 1)
	go func() {
		done <- uploader.Upload(context.Background(), transfer.TransferRequest{SourcePath: source, GUID: "window-overlap", ForceMultipart: true})
	}()

	select {
	case <-backend.overlapObserved:
	case <-time.After(10 * time.Second):
		close(releaseUploads)
		close(releaseSecondSign)
		<-done
		t.Fatal("next signing window did not overlap an active part upload")
	}
	if got := backend.partRequests(); len(got) != 2 || len(got[0]) != multipartPartSigningWindow || len(got[1]) != 1 {
		close(releaseUploads)
		close(releaseSecondSign)
		<-done
		t.Fatalf("signing requests = %v, want windows of 16 and 1", got)
	}
	close(releaseUploads)
	close(releaseSecondSign)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Upload() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Upload() did not finish after releasing uploads")
	}
	backend.mu.Lock()
	directAttempts := len(backend.directAttempts)
	fallbackCalls := backend.fallbackCalls
	backend.mu.Unlock()
	if directAttempts != 17 || fallbackCalls != 0 {
		t.Fatalf("future batch URL use: direct parts=%d fallback signs=%d, want 17 and 0", directAttempts, fallbackCalls)
	}
}

func TestMultipartSigningRetryRequestsFreshPartURL(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 101*common.MB)
	backend := newMultipartWindowBackend()
	backend.failDirectPart = 1
	uploader := &GenericUploader{Backend: backend}
	if err := uploader.Upload(context.Background(), transfer.TransferRequest{SourcePath: source, GUID: "window-retry", ForceMultipart: true}); err != nil {
		t.Fatalf("Upload() returned error: %v", err)
	}
	backend.mu.Lock()
	directAttempts := backend.directAttempts[1]
	fallbackCalls := backend.fallbackCalls
	backend.mu.Unlock()
	if directAttempts != 1 || fallbackCalls != 1 {
		t.Fatalf("part 1 direct attempts=%d fresh-sign attempts=%d, want 1 each", directAttempts, fallbackCalls)
	}
}

func TestMultipartLegacyResponseKeepsRemainingSigningConcurrent(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 110*common.MB)
	releaseUploads := make(chan struct{})
	backend := newMultipartWindowBackend()
	backend.legacyResponse = true
	backend.releaseUploads = releaseUploads
	uploader := &GenericUploader{Backend: backend}
	done := make(chan error, 1)
	go func() {
		done <- uploader.Upload(context.Background(), transfer.TransferRequest{SourcePath: source, GUID: "window-legacy", ForceMultipart: true})
	}()
	select {
	case <-backend.fallbackStarted:
	case <-time.After(10 * time.Second):
		close(releaseUploads)
		<-done
		t.Fatal("legacy server fallback did not keep part signing concurrent")
	}
	close(releaseUploads)
	if err := <-done; err != nil {
		t.Fatalf("Upload() returned error: %v", err)
	}
	if got := backend.partRequests(); len(got) != 1 {
		t.Fatalf("legacy server received %d batch attempts, want one", len(got))
	}
	if got := backend.fallbackCallCount(); got != 10 {
		t.Fatalf("legacy singleton fallback signing calls = %d, want 10 remaining parts", got)
	}
	backend.mu.Lock()
	firstPartAttempts := backend.directAttempts[1]
	backend.mu.Unlock()
	if firstPartAttempts != 1 {
		t.Fatalf("legacy first part direct attempts = %d, want the immediate singleton URL reused once", firstPartAttempts)
	}
}

func TestMultipartNearOrExpiredPrefetchedURLsUseFreshSigning(t *testing.T) {
	tests := []struct {
		name    string
		expires time.Duration
	}{
		{name: "expired", expires: -time.Second},
		{name: "inside safety margin", expires: 25 * time.Second},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
			source := createSparseMultipartSource(t, 101*common.MB)
			backend := newMultipartWindowBackend()
			backend.partExpiry = time.Now().Add(testCase.expires)
			uploader := &GenericUploader{Backend: backend}
			if err := uploader.Upload(context.Background(), transfer.TransferRequest{SourcePath: source, GUID: "window-expiry-" + testCase.name, ForceMultipart: true}); err != nil {
				t.Fatalf("Upload() returned error: %v", err)
			}
			backend.mu.Lock()
			directAttempts := len(backend.directAttempts)
			fallbackCalls := backend.fallbackCalls
			backend.mu.Unlock()
			if directAttempts != 0 || fallbackCalls != 11 {
				t.Fatalf("expired URL dispatch: direct parts=%d fresh-sign calls=%d, want 0 and 11", directAttempts, fallbackCalls)
			}
		})
	}
}

func TestMultipartBatchWithoutExpiryRefreshesFirstURLBeforePUT(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 101*common.MB)
	backend := newMultipartWindowBackend()
	backend.unknownBatchExpiry = true
	uploader := &GenericUploader{Backend: backend}
	if err := uploader.Upload(context.Background(), transfer.TransferRequest{SourcePath: source, GUID: "window-unknown-expiry", ForceMultipart: true}); err != nil {
		t.Fatalf("Upload() returned error: %v", err)
	}
	backend.mu.Lock()
	directAttempts := len(backend.directAttempts)
	fallbackCalls := backend.fallbackCalls
	backend.mu.Unlock()
	if directAttempts != 0 || fallbackCalls != 11 {
		t.Fatalf("unknown-expiry URL dispatch: direct parts=%d fresh-sign calls=%d, want 0 and 11", directAttempts, fallbackCalls)
	}
}

func TestMultipartCancellationWaitsForSigningProducer(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 170*common.MB)
	backend := newMultipartWindowBackend()
	backend.waitForActiveOnSecond = true
	backend.cancelSecondSign = true
	backend.releaseUploads = make(chan struct{})
	uploader := &GenericUploader{Backend: backend}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- uploader.Upload(ctx, transfer.TransferRequest{SourcePath: source, GUID: "window-cancel", ForceMultipart: true})
	}()
	select {
	case <-backend.overlapObserved:
	case <-time.After(10 * time.Second):
		cancel()
		<-done
		t.Fatal("signing producer did not reach the next window")
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("Upload() returned no error after cancellation")
	}
	select {
	case <-backend.signCanceled:
	default:
		t.Fatal("Upload() returned before the signing producer exited")
	}
}

func TestMultipartResumeDoesNotSignCompletedParts(t *testing.T) {
	t.Setenv("DATA_CLIENT_CACHE_DIR", t.TempDir())
	source := createSparseMultipartSource(t, 101*common.MB)
	req := testMultipartRequest(source, common.FileMetadata{})
	req.GUID = "window-resume"
	backend := newMultipartWindowBackend()
	uploader := &GenericUploader{Backend: backend}
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	state := checkpointStateForRequest(t, req, uploadCheckpointUploading, info)
	writeCheckpointForRequest(t, uploader, req, state)
	if err := uploader.Upload(context.Background(), req); err != nil {
		t.Fatalf("Upload() returned error: %v", err)
	}
	requests := backend.partRequests()
	want := make([]int32, 10)
	for i := range want {
		want[i] = int32(i + 2)
	}
	if len(requests) != 1 || fmt.Sprint(requests[0]) != fmt.Sprint(want) {
		t.Fatalf("signing requests = %v, want only incomplete parts %v", requests, want)
	}
}
