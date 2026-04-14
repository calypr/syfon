package upload

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/pkg/common"
	"github.com/calypr/syfon/client/pkg/logs"
	"github.com/calypr/syfon/client/pkg/request"
	"github.com/calypr/syfon/client/xfer"
)

type fakeGen3Upload struct {
	cred   *conf.Credential
	logger *logs.Gen3Logger
	doFunc func(context.Context, *request.RequestBuilder) (*http.Response, error)
}

func (f *fakeGen3Upload) Name() string             { return "fake" }
func (f *fakeGen3Upload) Logger() xfer.TransferLogger { return f.logger }

func (f *fakeGen3Upload) Do(ctx context.Context, req *request.RequestBuilder) (*http.Response, error) {
	return f.doFunc(ctx, req)
}
func (f *fakeGen3Upload) New(method, url string) *request.RequestBuilder {
	return &request.RequestBuilder{Method: method, Url: url}
}

func (f *fakeGen3Upload) ResolveUploadURL(ctx context.Context, guid string, filename string, metadata common.FileMetadata, bucket string) (string, error) {
	return "", fmt.Errorf("not implemented")
}
func (f *fakeGen3Upload) InitMultipartUpload(ctx context.Context, guid string, filename string, bucket string) (string, string, error) {
	resp, err := f.Do(ctx, &request.RequestBuilder{Url: common.FenceDataMultipartInitEndpoint})
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var msg struct {
		UploadID string `json:"uploadId"`
		GUID     string `json:"guid"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		return "", "", err
	}
	return msg.UploadID, msg.GUID, nil
}
func (f *fakeGen3Upload) GetMultipartUploadURL(ctx context.Context, key string, uploadID string, partNumber int32, bucket string) (string, error) {
	resp, err := f.Do(ctx, &request.RequestBuilder{Url: common.FenceDataMultipartUploadEndpoint})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var msg struct {
		PresignedURL string `json:"presigned_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&msg); err != nil {
		return "", err
	}
	return msg.PresignedURL, nil
}
func (f *fakeGen3Upload) CompleteMultipartUpload(ctx context.Context, key string, uploadID string, parts []internalapi.InternalMultipartPart, bucket string) error {
	_, err := f.Do(ctx, &request.RequestBuilder{Url: common.FenceDataMultipartCompleteEndpoint})
	return err
}
func (f *fakeGen3Upload) Upload(ctx context.Context, url string, body io.Reader, size int64) error {
	return nil
}
func (f *fakeGen3Upload) UploadPart(ctx context.Context, url string, body io.Reader, size int64) (string, error) {
	return "", nil
}
func (f *fakeGen3Upload) DeleteFile(ctx context.Context, guid string) (string, error) {
	return "", nil
}
func (f *fakeGen3Upload) CanonicalObjectURL(signedURL, bucketHint, fallbackDID string) (string, error) {
	return signedURL, nil
}

func TestMultipartUploadProgressIntegration(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		w.Header().Set("ETag", "etag-123")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	file, err := os.CreateTemp(t.TempDir(), "multipart-*.bin")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer file.Close()

	fileSize := int64(101 * common.MB)
	if err := file.Truncate(fileSize); err != nil {
		t.Fatalf("truncate file: %v", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("seek file: %v", err)
	}

	var (
		events []common.ProgressEvent
		mu     sync.Mutex
	)
	progress := func(event common.ProgressEvent) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, event)
		return nil
	}

	logger := logs.NewGen3Logger(nil, "", "")
	fake := &fakeGen3Upload{
		cred: &conf.Credential{
			APIEndpoint: "https://example.com",
			AccessToken: "token",
		},
		logger: logger,
		doFunc: func(_ context.Context, req *request.RequestBuilder) (*http.Response, error) {
			switch {
			case strings.Contains(req.Url, common.FenceDataMultipartInitEndpoint):
				return newJSONResponse(req.Url, `{"uploadId":"upload-123","guid":"guid-123"}`), nil
			case strings.Contains(req.Url, common.FenceDataMultipartUploadEndpoint):
				return newJSONResponse(req.Url, fmt.Sprintf(`{"presigned_url":"%s"}`, server.URL)), nil
			case strings.Contains(req.Url, common.FenceDataMultipartCompleteEndpoint):
				return newJSONResponse(req.Url, `{}`), nil
			default:
				return nil, fmt.Errorf("unexpected request url: %s", req.Url)
			}
		},
	}

	ctx = common.WithProgress(ctx, progress)
	ctx = common.WithOid(ctx, "guid-123")

	if err := MultipartUpload(ctx, fake, file.Name(), "multipart.bin", "guid-123", "bucket", common.FileMetadata{}, file, false); err != nil {
		t.Fatalf("multipart upload failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("expected progress events")
	}
	for i := 1; i < len(events); i++ {
		if events[i].BytesSoFar < events[i-1].BytesSoFar {
			t.Fatalf("bytesSoFar not monotonic: %d then %d", events[i-1].BytesSoFar, events[i].BytesSoFar)
		}
	}
	last := events[len(events)-1]
	if last.BytesSoFar != fileSize {
		t.Fatalf("expected final bytesSoFar %d, got %d", fileSize, last.BytesSoFar)
	}
}

func TestMultipartUploadResumesWithoutReinit(t *testing.T) {
	ctx := context.Background()

	var putCount atomic.Int64
	var failFirstPut atomic.Bool
	failFirstPut.Store(true)
	origTransport := http.DefaultClient.Transport
	http.DefaultClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPut {
			return &http.Response{
				StatusCode: http.StatusMethodNotAllowed,
				Body:       io.NopCloser(strings.NewReader("method not allowed")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()

		if failFirstPut.Load() && putCount.Load() == 0 {
			putCount.Add(1)
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(strings.NewReader("simulated failure")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}
		n := putCount.Add(1)
		h := make(http.Header)
		h.Set("ETag", fmt.Sprintf("etag-%d", n))
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     h,
			Request:    req,
		}, nil
	})
	defer func() { http.DefaultClient.Transport = origTransport }()

	tmp := t.TempDir()
	t.Setenv("DATA_CLIENT_CACHE_DIR", tmp)
	path := filepath.Join(tmp, "large.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	// Sparse file >100MB triggers multipart with multiple parts.
	if err := f.Truncate(120 * common.MB); err != nil {
		_ = f.Close()
		t.Fatalf("truncate temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}

	initCalls := 0
	completeCalls := 0
	logger := logs.NewGen3Logger(nil, "", "")
	fake := &fakeGen3Upload{
		cred: &conf.Credential{
			APIEndpoint: "https://example.com",
			AccessToken: "token",
		},
		logger: logger,
		doFunc: func(_ context.Context, req *request.RequestBuilder) (*http.Response, error) {
			switch {
			case strings.Contains(req.Url, common.FenceDataMultipartInitEndpoint):
				initCalls++
				return newJSONResponse(req.Url, `{"uploadId":"upload-resume-1","guid":"guid-resume-1"}`), nil
			case strings.Contains(req.Url, common.FenceDataMultipartUploadEndpoint):
				return newJSONResponse(req.Url, `{"presigned_url":"https://upload.invalid/part"}`), nil
			case strings.Contains(req.Url, common.FenceDataMultipartCompleteEndpoint):
				completeCalls++
				return newJSONResponse(req.Url, `{}`), nil
			default:
				return nil, fmt.Errorf("unexpected request url: %s", req.Url)
			}
		},
	}

	checkpointPath, err := multipartCheckpointPath(path, "resume.bin", "guid-resume-1", "bucket")
	if err != nil {
		t.Fatalf("checkpoint path: %v", err)
	}
	_ = os.Remove(checkpointPath)

	file1, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file1: %v", err)
	}
	err = MultipartUpload(ctx, fake, path, "resume.bin", "guid-resume-1", "bucket", common.FileMetadata{}, file1, false)
	_ = file1.Close()
	if err == nil {
		t.Fatal("expected first multipart upload to fail")
	}
	if initCalls != 1 {
		t.Fatalf("expected one init after first run, got %d", initCalls)
	}
	if _, statErr := os.Stat(checkpointPath); statErr != nil {
		t.Fatalf("expected checkpoint to exist after failure: %v", statErr)
	}

	failFirstPut.Store(false)
	file2, err := os.Open(path)
	if err != nil {
		t.Fatalf("open file2: %v", err)
	}
	err = MultipartUpload(ctx, fake, path, "resume.bin", "guid-resume-1", "bucket", common.FileMetadata{}, file2, false)
	_ = file2.Close()
	if err != nil {
		t.Fatalf("resume multipart upload failed: %v", err)
	}

	if initCalls != 1 {
		t.Fatalf("expected resume to reuse existing upload init; init calls = %d", initCalls)
	}
	if completeCalls != 1 {
		t.Fatalf("expected one complete call, got %d", completeCalls)
	}
	if _, statErr := os.Stat(checkpointPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected checkpoint cleanup after success, stat err: %v", statErr)
	}
}

func newJSONResponse(rawURL, body string) *http.Response {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		parsedURL = &url.URL{}
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Request:    &http.Request{URL: parsedURL},
		Header:     make(http.Header),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
