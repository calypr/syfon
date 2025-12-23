package main

import (
	"bufio"
	"bytes"
	"net"
	"net/http"

	"github.com/gin-gonic/gin"
)

type bufferingWriter struct {
	underlying gin.ResponseWriter

	header   http.Header
	status   int
	body     bytes.Buffer
	maxBody  int64
	tooLarge bool

	wroteHeader bool
	committed   bool
}

func newBufferingWriter(w gin.ResponseWriter, maxBody int64) *bufferingWriter {
	// Start with a copy of current headers (if any)
	h := make(http.Header)
	for k, v := range w.Header() {
		vs := make([]string, len(v))
		copy(vs, v)
		h[k] = vs
	}
	return &bufferingWriter{
		underlying: w,
		header:     h,
		maxBody:    maxBody,
	}
}

func (w *bufferingWriter) Header() http.Header {
	return w.header
}

func (w *bufferingWriter) WriteHeader(statusCode int) {
	if w.committed {
		return
	}
	w.status = statusCode
	w.wroteHeader = true
}

func (w *bufferingWriter) Write(p []byte) (int, error) {
	if w.committed {
		return w.underlying.Write(p)
	}
	// Default status if never set
	if !w.wroteHeader && w.status == 0 {
		w.status = http.StatusOK
		w.wroteHeader = true
	}

	// Enforce max buffer
	if w.maxBody > 0 && int64(w.body.Len()+len(p)) > w.maxBody {
		w.tooLarge = true
		// We still buffer up to maxBody, and then ignore the rest (or you can stop buffering entirely)
		remaining := int(w.maxBody - int64(w.body.Len()))
		if remaining > 0 {
			_, _ = w.body.Write(p[:remaining])
		}
		return len(p), nil
	}

	_, _ = w.body.Write(p)
	return len(p), nil
}

func (w *bufferingWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

// commit writes the buffered response to the underlying writer exactly once.
func (w *bufferingWriter) commit() error {
	if w.committed {
		return nil
	}
	w.committed = true

	// Copy headers
	dst := w.underlying.Header()
	for k := range dst {
		dst.Del(k)
	}
	for k, v := range w.header {
		vs := make([]string, len(v))
		copy(vs, v)
		dst[k] = vs
	}

	// Write status + body
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.underlying.WriteHeader(w.status)
	_, err := w.underlying.Write(w.body.Bytes())
	return err
}

// Gin interfaces passthrough (important in production)
func (w *bufferingWriter) Status() int {
	if w.status == 0 {
		return w.underlying.Status()
	}
	return w.status
}
func (w *bufferingWriter) Size() int                { return w.body.Len() }
func (w *bufferingWriter) Written() bool            { return w.wroteHeader || w.body.Len() > 0 }
func (w *bufferingWriter) WriteHeaderNow()          { /* no-op until commit */ }
func (w *bufferingWriter) Pusher() http.Pusher      { return w.underlying.Pusher() }
func (w *bufferingWriter) Flush()                   { /* buffering: no flush until commit */ }
func (w *bufferingWriter) CloseNotify() <-chan bool { return w.underlying.CloseNotify() }
func (w *bufferingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.underlying.Hijack()
}
