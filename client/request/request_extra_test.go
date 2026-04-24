package request

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/conf"
	"github.com/calypr/syfon/client/logs"
)

func TestResponseErrorErrorString(t *testing.T) {
	t.Parallel()

	err := (&ResponseError{Method: http.MethodGet, URL: "https://example.test/data", Status: http.StatusForbidden, Body: "denied"}).Error()
	if err != "GET https://example.test/data: status 403 body=denied" {
		t.Fatalf("unexpected error string: %q", err)
	}
}

func TestRequestDo_ResponseAndDecodeErrors(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	baseClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/forbidden":
			return &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Body: io.NopCloser(strings.NewReader(" denied ")), Header: make(http.Header), Request: r}, nil
		case "/badjson":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{")), Header: make(http.Header), Request: r}, nil
		default:
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header), Request: r}, nil
		}
	})}
	req := NewBasicAuthRequestor(logs.NewGen3Logger(logger, "", ""), nil, &mockConfigManager{}, "https://example.test", "ua", baseClient)

	var out map[string]any
	err := req.Do(context.Background(), http.MethodGet, "/forbidden", nil, &out)
	if err == nil {
		t.Fatal("expected ResponseError")
	}
	respErr, ok := err.(*ResponseError)
	if !ok {
		t.Fatalf("expected *ResponseError, got %T", err)
	}
	if respErr.Status != http.StatusForbidden || respErr.Method != http.MethodGet || respErr.Body != "denied" {
		t.Fatalf("unexpected response error details: %+v", respErr)
	}

	err = req.Do(context.Background(), http.MethodGet, "/badjson", nil, &out)
	if err == nil || !strings.Contains(err.Error(), "decode response") {
		t.Fatalf("expected decode response error, got %v", err)
	}
}

func TestRequestDo_BasicAuthAndPartSize(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	cred := &conf.Credential{KeyID: "kid", APIKey: "secret"}
	baseClient := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		expectedBasic := "Basic " + base64.StdEncoding.EncodeToString([]byte("kid:secret"))
		if got := r.Header.Get("Authorization"); got != expectedBasic {
			t.Fatalf("expected basic auth header %q, got %q", expectedBasic, got)
		}
		if r.ContentLength != 5 {
			t.Fatalf("expected content length 5, got %d", r.ContentLength)
		}
		if ua := r.Header.Get("User-Agent"); ua != "ua" {
			t.Fatalf("expected user-agent header, got %q", ua)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header), Request: r}, nil
	})}
	req := NewBasicAuthRequestor(logs.NewGen3Logger(logger, "", ""), cred, &mockConfigManager{}, "https://example.test", "ua", baseClient)

	payload := strings.NewReader("hello")
	if err := req.Do(context.Background(), http.MethodPut, "/upload", payload, nil, WithPartSize(5)); err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
}
