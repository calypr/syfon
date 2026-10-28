package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func TestNativeHTTPHandlerBoundsBodiesAndPreservesRepeatedHeadersAndCookies(t *testing.T) {
	app := fiber.New(fiber.Config{BodyLimit: 32})
	var handled atomic.Int64
	server := httptest.NewServer(NewNativeHTTPHandler(app, func() {
		app.Post("/echo", func(c fiber.Ctx) error {
			handled.Add(1)
			c.Set("Set-Cookie", "first=1")
			c.Response().Header.Add("Set-Cookie", "second=2")
			c.Response().Header.Add("X-Reply", "one")
			c.Response().Header.Add("X-Reply", "two")
			return c.SendString(strings.Join(c.GetHeaders()["X-Trace"], "|"))
		})
	}))
	t.Cleanup(server.Close)

	request, err := http.NewRequest(http.MethodPost, server.URL+"/echo", strings.NewReader("ok"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Add("X-Trace", "one")
	request.Header.Add("X-Trace", "two")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK || string(body) != "one|two" {
		t.Fatalf("response = (%d, %q), want 200 and both request header values", response.StatusCode, body)
	}
	if got := response.Header.Values("Set-Cookie"); len(got) != 2 || got[0] != "first=1" || got[1] != "second=2" {
		t.Fatalf("Set-Cookie values = %v, want both cookie fields", got)
	}
	if got := response.Header.Values("X-Reply"); len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Fatalf("X-Reply values = %v, want both response fields", got)
	}

	largeJSON := `{"payload":"` + strings.Repeat("x", 24) + `"}`
	oversized, err := http.NewRequest(http.MethodPost, server.URL+"/echo", strings.NewReader(largeJSON))
	if err != nil {
		t.Fatal(err)
	}
	oversized.Header.Set("Content-Type", "application/json")
	response, err = server.Client().Do(oversized)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized JSON status = %d, want 413", response.StatusCode)
	}
	if handled.Load() != 1 {
		t.Fatalf("handler calls = %d, want only the bounded request to reach it", handled.Load())
	}

	chunked, err := http.NewRequest(http.MethodPost, server.URL+"/echo", io.NopCloser(strings.NewReader(largeJSON)))
	if err != nil {
		t.Fatal(err)
	}
	chunked.Header.Set("Content-Type", "application/json")
	response, err = server.Client().Do(chunked)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge || handled.Load() != 1 {
		t.Fatalf("chunked oversized request = status %d, handler calls %d; want 413 and no additional handler call", response.StatusCode, handled.Load())
	}

	unknownUploadPath, err := http.NewRequest(http.MethodPut, server.URL+"/info/lfs/objects/id/extra", strings.NewReader(largeJSON))
	if err != nil {
		t.Fatal(err)
	}
	response, err = server.Client().Do(unknownUploadPath)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body on a non-upload route status = %d, want 413", response.StatusCode)
	}
}

func TestNativeHTTPHandlerPreservesTLSMetadata(t *testing.T) {
	app := fiber.New()
	server := httptest.NewTLSServer(NewNativeHTTPHandler(app, func() {
		app.Get("/scheme", func(c fiber.Ctx) error {
			if c.RequestCtx().TLSConnectionState() == nil {
				return c.SendStatus(http.StatusInternalServerError)
			}
			return c.SendString(c.Scheme() + "|" + c.IP())
		})
	}))
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + "/scheme")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(string(body), "https|") || strings.TrimPrefix(string(body), "https|") == "" {
		t.Fatalf("TLS response = (%d, %q), want https and a remote IP", response.StatusCode, body)
	}
}

type closeTrackingReader struct {
	reader     io.Reader
	closed     atomic.Bool
	closeCalls atomic.Int64
}

func (r *closeTrackingReader) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *closeTrackingReader) Close() error {
	r.closeCalls.Add(1)
	r.closed.Store(true)
	return nil
}

type failingNativeResponseWriter struct {
	header http.Header
}

func (w *failingNativeResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (*failingNativeResponseWriter) WriteHeader(int) {}
func (*failingNativeResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("client disconnected")
}

func TestNativeHTTPHandlerClosesResponseStreamAfterWriteFailure(t *testing.T) {
	stream := &closeTrackingReader{reader: strings.NewReader("streamed body")}
	app := fiber.New()
	handler := NewNativeHTTPHandler(app, func() {
		app.Get("/stream", func(c fiber.Ctx) error {
			c.Response().SetBodyStream(stream, -1)
			return nil
		})
	})
	handler.ServeHTTP(&failingNativeResponseWriter{}, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if !stream.closed.Load() || stream.closeCalls.Load() != 1 {
		t.Fatalf("response stream close count = %d, want exactly one close after the client write failed", stream.closeCalls.Load())
	}
}

type trackingPipeReader struct {
	reader     *io.PipeReader
	started    chan struct{}
	startOnce  atomic.Bool
	closeCalls atomic.Int64
}

func (r *trackingPipeReader) Read(p []byte) (int, error) {
	if r.startOnce.CompareAndSwap(false, true) {
		close(r.started)
	}
	return r.reader.Read(p)
}

func (r *trackingPipeReader) Close() error {
	r.closeCalls.Add(1)
	return r.reader.Close()
}

func TestNativeHTTPHandlerCancelsBlockedResponseStream(t *testing.T) {
	pipeReader, pipeWriter := io.Pipe()
	defer pipeWriter.Close()
	stream := &trackingPipeReader{reader: pipeReader, started: make(chan struct{})}
	app := fiber.New()
	handler := NewNativeHTTPHandler(app, func() {
		app.Get("/stream", func(c fiber.Ctx) error {
			c.Response().SetBodyStream(stream, -1)
			return nil
		})
	})
	requestContext, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(requestContext)
	result := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), request)
		close(result)
	}()
	select {
	case <-stream.started:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("response copy did not start reading its stream")
	}
	cancel()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("request cancellation did not release the blocked response stream")
	}
	if got := stream.closeCalls.Load(); got != 1 {
		t.Fatalf("response stream close count = %d, want exactly one", got)
	}
}
