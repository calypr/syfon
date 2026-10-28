package httpapi

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/valyala/fasthttp"
)

type nativeRequestContextKey struct{}

var errNativeBridgeRawWrite = errors.New("raw connection writes are unavailable through the native HTTP bridge")

func nativeContextMiddleware() fiber.Handler {
	return func(c fiber.Ctx) error {
		if ctx, ok := c.RequestCtx().UserValue(nativeRequestContextKey{}).(context.Context); ok && ctx != nil {
			c.SetContext(ctx)
		}
		return c.Next()
	}
}

// NewNativeHTTPHandler installs the native request context middleware, builds
// the supplied routes, and serves the existing Fiber app through net/http.
func NewNativeHTTPHandler(app *fiber.App, registerRoutes func()) http.Handler {
	app.Use(nativeContextMiddleware())
	if registerRoutes != nil {
		registerRoutes()
	}
	handler := app.Handler()
	bodyLimit := int64(app.Config().BodyLimit)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamBody := isLFSObjectUpload(r)
		if r.Body != nil && !streamBody {
			defer r.Body.Close()
		}

		var body []byte
		if !streamBody && r.Body != nil && r.Body != http.NoBody {
			if r.ContentLength > bodyLimit {
				http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
			readLimit := bodyLimit
			if readLimit < math.MaxInt64 {
				readLimit++
			}
			limited := io.LimitReader(r.Body, readLimit)
			var err error
			body, err = io.ReadAll(limited)
			if err != nil {
				if r.Context().Err() == nil {
					http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				}
				return
			}
			if int64(len(body)) > bodyLimit {
				http.Error(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
				return
			}
		}

		request := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(request)
		request.Header.SetMethod(r.Method)
		requestURI := r.RequestURI
		if requestURI == "" && r.URL != nil {
			requestURI = r.URL.RequestURI()
		}
		request.SetRequestURI(requestURI)
		request.SetHost(r.Host)
		request.Header.SetHost(r.Host)
		request.Header.SetProtocol(fasthttpProtocol(r))
		if r.ContentLength >= 0 {
			request.Header.SetContentLength(int(r.ContentLength))
		} else if len(r.TransferEncoding) > 0 {
			request.Header.SetContentLength(-1)
		}
		for key, values := range r.Header {
			if len(values) == 0 || isFramingRequestHeader(key) {
				continue
			}
			request.Header.Set(key, values[0])
			for _, value := range values[1:] {
				request.Header.Add(key, value)
			}
		}
		if !streamBody {
			request.SetBody(body)
		}

		fasthttpCtx := new(fasthttp.RequestCtx)
		conn := newNativeBridgeConn(r)
		// Init2 preserves bridged TLS and address metadata. CopyTo leaves the LFS body stream unattached until below.
		fasthttpCtx.Init2(conn, nil, true)
		request.CopyTo(&fasthttpCtx.Request)
		fasthttpCtx.SetUserValue(nativeRequestContextKey{}, r.Context())
		if streamBody && r.Body != nil && r.Body != http.NoBody {
			fasthttpCtx.Request.SetBodyStream(r.Body, int(r.ContentLength))
			defer fasthttpCtx.Request.CloseBodyStream()
		}
		defer fasthttpCtx.ResetUserValues()

		handler(fasthttpCtx)
		writeNativeResponse(w, r.Context(), fasthttpCtx)
	})
}

func isLFSObjectUpload(r *http.Request) bool {
	if r.Method != http.MethodPut || r.URL == nil {
		return false
	}
	const uploadPrefix = "/info/lfs/objects/"
	if !strings.HasPrefix(r.URL.Path, uploadPrefix) {
		return false
	}
	objectID := strings.TrimPrefix(r.URL.Path, uploadPrefix)
	return objectID != "" && !strings.Contains(objectID, "/")
}

func isFramingRequestHeader(name string) bool {
	switch strings.ToLower(name) {
	case "host", "content-length", "transfer-encoding":
		return true
	default:
		return false
	}
}

func fasthttpProtocol(r *http.Request) string {
	switch r.ProtoMajor {
	case 2:
		return "HTTP/2"
	case 3:
		return "HTTP/3"
	case 0:
		return "HTTP/1.1"
	default:
		return r.Proto
	}
}

func writeNativeResponse(w http.ResponseWriter, requestContext context.Context, ctx *fasthttp.RequestCtx) {
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Now().Add(120 * time.Second)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return
	}
	response := &ctx.Response
	response.Header.VisitAll(func(key, value []byte) {
		w.Header().Add(string(key), string(value))
	})
	w.WriteHeader(response.StatusCode())

	if responseBody := response.BodyStream(); responseBody != nil {
		if closer, ok := responseBody.(io.Closer); ok {
			cancelCallbackDone := make(chan struct{})
			stopCancelCallback := context.AfterFunc(requestContext, func() {
				defer close(cancelCallbackDone)
				_ = closer.Close()
			})
			defer func() {
				if stopCancelCallback() {
					_ = response.CloseBodyStream()
					return
				}
				<-cancelCallbackDone
			}()
		} else {
			defer response.CloseBodyStream()
		}
		buffer := make([]byte, 32*1024)
		for {
			count, readErr := responseBody.Read(buffer)
			if count > 0 {
				if _, err := w.Write(buffer[:count]); err != nil {
					return
				}
				if err := controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}
	if _, err := w.Write(response.Body()); err != nil {
		return
	}
}

type nativeBridgeConn struct {
	remote net.Addr
	local  net.Addr
	tls    *tls.ConnectionState
}

type nativeBridgeTLSConn struct {
	*nativeBridgeConn
}

func newNativeBridgeConn(r *http.Request) net.Conn {
	remote, err := net.ResolveTCPAddr("tcp", r.RemoteAddr)
	if err != nil {
		remote = &net.TCPAddr{}
	}
	local, _ := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
	if local == nil {
		local = &net.TCPAddr{}
	}
	conn := &nativeBridgeConn{remote: remote, local: local, tls: r.TLS}
	if r.TLS != nil {
		return &nativeBridgeTLSConn{nativeBridgeConn: conn}
	}
	return conn
}

func (*nativeBridgeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (*nativeBridgeConn) Write([]byte) (int, error)        { return 0, errNativeBridgeRawWrite }
func (*nativeBridgeConn) Close() error                     { return nil }
func (c *nativeBridgeConn) LocalAddr() net.Addr            { return c.local }
func (c *nativeBridgeConn) RemoteAddr() net.Addr           { return c.remote }
func (*nativeBridgeConn) SetDeadline(time.Time) error      { return nil }
func (*nativeBridgeConn) SetReadDeadline(time.Time) error  { return nil }
func (*nativeBridgeConn) SetWriteDeadline(time.Time) error { return nil }
func (*nativeBridgeTLSConn) Handshake() error              { return nil }
func (c *nativeBridgeTLSConn) ConnectionState() tls.ConnectionState {
	return *c.tls
}
