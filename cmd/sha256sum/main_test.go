package sha256sum

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	syclient "github.com/calypr/syfon/client"
)

func TestHashURLReadsFileAndReturnsSHA256(t *testing.T) {
	payload := []byte("sha256sum file payload")
	path := filepath.Join(t.TempDir(), "payload.bin")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	rawURL := (&url.URL{Scheme: "file", Path: path}).String()

	got, err := hashURL(context.Background(), rawURL, nil)
	if err != nil {
		t.Fatalf("hashURL returned error: %v", err)
	}
	if want := checksum(payload); got != want {
		t.Fatalf("hashURL = %s, want %s", got, want)
	}
}

func TestHashURLHTTPClosesBodyAndReturnsSHA256(t *testing.T) {
	payload := "sha256sum http payload"
	body := &trackedBody{reader: strings.NewReader(payload)}
	client := newHashTestClient(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	}))

	got, err := hashURL(context.Background(), "https://source.example/object", client)
	if err != nil {
		t.Fatalf("hashURL returned error: %v", err)
	}
	if want := checksum([]byte(payload)); got != want {
		t.Fatalf("hashURL = %s, want %s", got, want)
	}
	if !body.closed {
		t.Fatal("hashURL did not close the HTTP response body")
	}
}

func TestHashURLHTTPUsesBoundedReadBuffer(t *testing.T) {
	const payloadSize = 4 << 20
	const maxReadBuffer = 64 << 10
	reader := &boundedReadReader{remaining: payloadSize, maxBuffer: maxReadBuffer}
	body := &trackedBody{reader: reader}
	client := newHashTestClient(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	}))

	got, err := hashURL(context.Background(), "https://source.example/object", client)
	if err != nil {
		t.Fatalf("hashURL returned error: %v", err)
	}
	if want := checksum(bytes.Repeat([]byte{'x'}, payloadSize)); got != want {
		t.Fatalf("hashURL = %s, want %s", got, want)
	}
	if reader.maxRequested > maxReadBuffer {
		t.Fatalf("hashURL requested %d-byte read buffer, limit is %d", reader.maxRequested, maxReadBuffer)
	}
	if !body.closed {
		t.Fatal("hashURL did not close the HTTP response body")
	}
}

func TestHashURLHTTPReadHonorsCancellationAndClosesBody(t *testing.T) {
	bodyReady := make(chan *contextReadCloser, 1)
	client := newHashTestClient(t, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		body := &contextReadCloser{ctx: req.Context(), started: make(chan struct{})}
		bodyReady <- body
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
			Request:    req,
		}, nil
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := hashURL(ctx, "https://source.example/object", client)
		done <- err
	}()

	var body *contextReadCloser
	select {
	case body = <-bodyReady:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("hashURL did not receive an HTTP response")
	}
	select {
	case <-body.started:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("hashURL did not start reading the response body")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("hashURL error = %v, want context cancellation", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hashURL did not stop after request cancellation")
	}
	if !body.closed.Load() {
		t.Fatal("hashURL did not close the canceled HTTP response body")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type trackedBody struct {
	reader io.Reader
	closed bool
}

func (b *trackedBody) Read(p []byte) (int, error) { return b.reader.Read(p) }

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

type contextReadCloser struct {
	ctx     context.Context
	started chan struct{}
	once    sync.Once
	closed  atomic.Bool
}

func (b *contextReadCloser) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *contextReadCloser) Close() error {
	b.closed.Store(true)
	return nil
}

type boundedReadReader struct {
	remaining    int
	maxBuffer    int
	maxRequested int
}

func (r *boundedReadReader) Read(p []byte) (int, error) {
	if len(p) > r.maxBuffer {
		return 0, fmt.Errorf("read buffer %d exceeds limit %d", len(p), r.maxBuffer)
	}
	if len(p) > r.maxRequested {
		r.maxRequested = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > r.remaining {
		n = r.remaining
	}
	for i := 0; i < n; i++ {
		p[i] = 'x'
	}
	r.remaining -= n
	return n, nil
}

func newHashTestClient(t *testing.T, transport http.RoundTripper) *syclient.Client {
	t.Helper()
	client, err := syclient.New("https://api.example", syclient.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("create test client: %v", err)
	}
	return client
}

func checksum(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
