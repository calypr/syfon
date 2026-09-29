package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/apierror"
)

func TestMultipartAbortUsesGeneratedClient(t *testing.T) {
	requester := &recordingRequester{
		response: &http.Response{
			StatusCode: http.StatusNoContent,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
		},
	}
	generated, err := internalapi.NewClientWithResponses("http://example.test", internalapi.WithHTTPClient(requester), internalapi.WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		req.Header.Set("X-Request-Editor", "applied")
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	if err := NewDataService(generated, nil, nil, nil).MultipartAbort(context.Background(), "upload-123"); err != nil {
		t.Fatalf("MultipartAbort returned error: %v", err)
	}
	if requester.method != http.MethodPost || requester.path != "/data/multipart/abort" {
		t.Fatalf("request = %s %s, want POST /data/multipart/abort", requester.method, requester.path)
	}
	if got := requester.request.Header.Get("X-Request-Editor"); got != "applied" {
		t.Fatalf("request editor header = %q, want applied", got)
	}
	var body internalapi.InternalMultipartAbortRequest
	if err := json.Unmarshal(requester.body, &body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if body.UploadId != "upload-123" {
		t.Fatalf("request upload ID = %q, want upload-123", body.UploadId)
	}
}

func TestMultipartAbortPreservesGeneratedErrors(t *testing.T) {
	t.Run("transport", func(t *testing.T) {
		cause := errors.New("transport failed")
		requester := &recordingRequester{err: cause}
		generated, err := internalapi.NewClientWithResponses("http://example.test", internalapi.WithHTTPClient(requester))
		if err != nil {
			t.Fatal(err)
		}
		if err := NewDataService(generated, nil, nil, nil).MultipartAbort(context.Background(), "upload-123"); !errors.Is(err, cause) {
			t.Fatalf("MultipartAbort error = %v, want transport cause", err)
		}
	})

	t.Run("api response", func(t *testing.T) {
		requester := &recordingRequester{
			response: &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"code":"access_denied","message":"denied"}`)),
			},
		}
		generated, err := internalapi.NewClientWithResponses("http://example.test", internalapi.WithHTTPClient(requester))
		if err != nil {
			t.Fatal(err)
		}
		err = NewDataService(generated, nil, nil, nil).MultipartAbort(context.Background(), "upload-123")
		var apiErr *apierror.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != http.StatusForbidden {
			t.Fatalf("MultipartAbort error = %T %v, want 403 APIError", err, err)
		}
	})
}
