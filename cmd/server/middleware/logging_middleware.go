// Package main contains the HTTP server entrypoint and middleware, including
// logging middleware that redacts sensitive authentication headers and logs
// both request and response metadata.
package middleware

import (
	"bytes"
	"io"
	"log"

	"github.com/gin-gonic/gin"
)

// bodyLogWriter wraps gin.ResponseWriter to capture the response body
// while still writing it through to the underlying writer.
//
// It is used by logging middleware to log the response payload after
// the handler has executed.
type bodyLogWriter struct {
	gin.ResponseWriter
	// body accumulates the bytes written to the response.
	body *bytes.Buffer
}

// Write implements the io.Writer interface for bodyLogWriter.
//
// It appends the written bytes to the internal buffer so they can be
// logged later, and then forwards the write to the embedded
// gin.ResponseWriter so the client still receives the response.
func (w bodyLogWriter) Write(b []byte) (int, error) {
	// Capture response body.
	w.body.Write(b)
	// Write through to original writer.
	return w.ResponseWriter.Write(b)
}

// RequestLogRedactingAuth returns a gin.HandlerFunc middleware that logs
// incoming HTTP requests and outgoing HTTP responses while redacting
// sensitive authentication headers.
//
// Behavior:
//   - Clones the incoming request headers and replaces values for
//     `Authorization` and `X-Api-Key` with the literal value `[REDACTED]`
//     before logging.
//   - Logs the request method, path, query string, and sanitized headers
//     at the start of the request.
//   - Wraps gin's ResponseWriter to capture the response body.
//   - After the handler chain completes (c.Next), logs the response status
//     code, path, and the captured response body.
//
// This middleware is best placed early in the gin engine's middleware
// chain so that it can observe all modifications performed by subsequent
// handlers.
func RequestLogRedactingAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Clone and sanitize headers to avoid logging secrets.
		h := c.Request.Header.Clone()
		if h.Get("Authorization") != "" {
			h.Set("Authorization", "[REDACTED]")
		}
		if h.Get("X-Api-Key") != "" {
			h.Set("X-Api-Key", "[REDACTED]")
		}

		// Read and log request body, then restore it.
		var reqBodyBuf bytes.Buffer
		if c.Request.Body != nil {
			_, _ = io.Copy(&reqBodyBuf, c.Request.Body)
			// Restore body for handlers.
			c.Request.Body = io.NopCloser(bytes.NewReader(reqBodyBuf.Bytes()))
		}

		log.Printf("http request method=%s path=%s query=%s headers=%v body=%s",
			c.Request.Method, c.Request.URL.Path, c.Request.URL.RawQuery, h, reqBodyBuf.String())

		// Wrap ResponseWriter to capture body for logging.
		blw := &bodyLogWriter{
			ResponseWriter: c.Writer,
			body:           &bytes.Buffer{},
		}
		c.Writer = blw

		// Proceed with the remaining handlers.
		c.Next()

		log.Printf("http response status=%d path=%s body=%s",
			c.Writer.Status(), c.Request.URL.Path, blw.body.String())
	}
}
