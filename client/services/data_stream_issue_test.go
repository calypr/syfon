package services

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/errorapi"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/apierror"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/request"
	"github.com/calypr/syfon/client/transfer"
)

type closeTrackingReader struct {
	reader *strings.Reader
	closed bool
}

func (r *closeTrackingReader) Read(p []byte) (int, error) { return r.reader.Read(p) }

func (r *closeTrackingReader) Close() error {
	r.closed = true
	return nil
}

type downloadResponseRequester struct {
	response *http.Response
}

func (r *downloadResponseRequester) Do(req *http.Request) (*http.Response, error) {
	if r.response == nil {
		return nil, nil
	}
	r.response.Request = req
	return r.response, nil
}

func newDownloadBoundaryService(t *testing.T, requester request.HTTPDoer) *DataService {
	t.Helper()
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		payload, err := json.Marshal(internalapi.InternalSignedURL{Url: ptrString("https://storage.example/object?X-Amz-Signature=secret")})
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(string(payload))),
			Request:    req,
		}, nil
	})
	client, err := internalapi.NewClientWithResponses("https://api.example", internalapi.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("NewClientWithResponses returned error: %v", err)
	}
	return NewDataService(client, requester, discardLogger(), nil)
}

func TestDataServiceReadersRejectFailedResponsesAndCloseBodies(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			body := &closeTrackingReader{reader: strings.NewReader(strings.Repeat("x", maxDownloadErrorPreview+100))}
			service := newDownloadBoundaryService(t, &downloadResponseRequester{response: &http.Response{
				StatusCode: status,
				Header:     make(http.Header),
				Body:       body,
			}})

			reader, err := service.GetReader(context.Background(), "object")
			if reader != nil {
				t.Fatal("GetReader returned a body for a failed response")
			}
			if err == nil {
				t.Fatal("GetReader returned nil error for a failed response")
			}
			var responseErr *apierror.APIError
			if !errors.As(err, &responseErr) {
				t.Fatalf("GetReader error = %T %v, want typed response error", err, err)
			}
			if len(responseErr.Body) > maxDownloadErrorPreview {
				t.Fatalf("failed body preview length = %d, want at most %d", len(responseErr.Body), maxDownloadErrorPreview)
			}
			if !body.closed {
				t.Fatal("failed response body was not closed")
			}
			if strings.Contains(responseErr.URL, "Signature") || strings.Contains(responseErr.URL, "secret") || strings.Contains(responseErr.URL, "?") {
				t.Fatalf("signed response URL was not redacted: %q", responseErr.URL)
			}
			if status == http.StatusNotFound && !errors.Is(err, errorapi.ErrNotFound) {
				t.Fatalf("404 error lost not-found classification: %v", err)
			}
		})
	}
}

func TestDataServiceReadersRejectNilResponsesAndBodies(t *testing.T) {
	t.Run("nil response", func(t *testing.T) {
		service := newDownloadBoundaryService(t, &downloadResponseRequester{response: nil})
		requester := service.httpClient.(*downloadResponseRequester)
		requester.response = nil
		reader, err := service.GetReader(context.Background(), "object")
		if reader != nil || err == nil || !strings.Contains(err.Error(), "download response is nil") {
			t.Fatalf("GetReader returned reader=%v err=%v, want nil response error", reader, err)
		}
	})

	t.Run("nil body", func(t *testing.T) {
		service := newDownloadBoundaryService(t, &downloadResponseRequester{response: &http.Response{StatusCode: http.StatusPartialContent}})
		reader, err := service.GetReader(context.Background(), "object")
		if reader != nil || err == nil || !strings.Contains(err.Error(), "download response body is nil") {
			t.Fatalf("GetReader returned reader=%v err=%v, want nil body error", reader, err)
		}
	})
}

type redirectDownloadTransport struct {
	hosts  []string
	bodies []*closeTrackingReader
}

func (t *redirectDownloadTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.hosts = append(t.hosts, req.URL.Host)
	status := http.StatusOK
	body := "downloaded data"
	header := make(http.Header)
	if req.URL.Host == "storage.example" {
		status = http.StatusFound
		body = "redirect response"
		header.Set("Location", "https://cdn.example/object")
	}
	responseBody := &closeTrackingReader{reader: strings.NewReader(body)}
	t.bodies = append(t.bodies, responseBody)
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       responseBody,
		Request:    req,
	}, nil
}

func TestDataServiceGetReaderChecksFinalStatusAndKeepsRedirectPolicy(t *testing.T) {
	t.Run("caller does not follow redirects", func(t *testing.T) {
		transport := &redirectDownloadTransport{}
		client := &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		service := newDownloadBoundaryService(t, client)

		reader, err := service.GetReader(context.Background(), "object")
		if reader != nil || err == nil {
			t.Fatalf("GetReader returned reader=%v err=%v, want redirect error", reader, err)
		}
		if len(transport.hosts) != 1 || transport.hosts[0] != "storage.example" {
			t.Fatalf("redirect policy made requests to %v, want only storage.example", transport.hosts)
		}
		if len(transport.bodies) != 1 || !transport.bodies[0].closed {
			t.Fatal("no-follow redirect response body was not closed")
		}
	})

	t.Run("caller follows redirects", func(t *testing.T) {
		transport := &redirectDownloadTransport{}
		service := newDownloadBoundaryService(t, &http.Client{Transport: transport})
		reader, err := service.GetReader(context.Background(), "object")
		if err != nil {
			t.Fatalf("GetReader returned error: %v", err)
		}
		defer reader.Close()
		data, err := io.ReadAll(reader)
		if err != nil || string(data) != "downloaded data" {
			t.Fatalf("followed response data=%q err=%v", data, err)
		}
		if len(transport.hosts) != 2 || transport.hosts[0] != "storage.example" || transport.hosts[1] != "cdn.example" {
			t.Fatalf("redirect policy made requests to %v, want storage.example then cdn.example", transport.hosts)
		}
	})
}

func TestDataServiceRangeReaderRejectsInvalidContentRange(t *testing.T) {
	for _, test := range []struct {
		name         string
		contentRange string
	}{
		{name: "missing"},
		{name: "malformed", contentRange: "bytes 4-x/10"},
		{name: "wrong unit", contentRange: "items 4-7/10"},
		{name: "shifted start", contentRange: "bytes 3-6/10"},
		{name: "incomplete before end", contentRange: "bytes 4-6/10"},
		{name: "extends beyond requested end", contentRange: "bytes 4-8/10"},
		{name: "total does not include end", contentRange: "bytes 4-7/7"},
		{name: "unknown total", contentRange: "bytes 4-7/*"},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := &closeTrackingReader{reader: strings.NewReader("part")}
			header := make(http.Header)
			if test.contentRange != "" {
				header.Set("Content-Range", test.contentRange)
			}
			service := newDownloadBoundaryService(t, &downloadResponseRequester{response: &http.Response{
				StatusCode: http.StatusPartialContent,
				Header:     header,
				Body:       body,
			}})

			reader, err := service.GetRangeReader(context.Background(), "object", 4, 4)
			if reader != nil || err == nil || !strings.Contains(err.Error(), "Content-Range") {
				t.Fatalf("GetRangeReader returned reader=%v err=%v, want Content-Range error", reader, err)
			}
			if !body.closed {
				t.Fatal("invalid range response body was not closed")
			}
		})
	}
}

func TestDataServiceRangeReaderAllowsRangeClampedAtKnownEOF(t *testing.T) {
	body := &closeTrackingReader{reader: strings.NewReader("end")}
	header := make(http.Header)
	header.Set("Content-Range", "bytes 4-6/7")
	service := newDownloadBoundaryService(t, &downloadResponseRequester{response: &http.Response{
		StatusCode: http.StatusPartialContent,
		Header:     header,
		Body:       body,
	}})

	reader, err := service.GetRangeReader(context.Background(), "object", 4, 4)
	if err != nil {
		t.Fatalf("GetRangeReader returned error for a range clamped at known EOF: %v", err)
	}
	data, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil || closeErr != nil || string(data) != "end" {
		t.Fatalf("range response data=%q read error=%v close error=%v", data, err, closeErr)
	}
}

func TestDataServiceRangeReaderChecksFailureBeforeRangeFallback(t *testing.T) {
	failedBody := &closeTrackingReader{reader: strings.NewReader("error document")}
	service := newDownloadBoundaryService(t, &downloadResponseRequester{response: &http.Response{
		StatusCode: http.StatusInternalServerError,
		Header:     make(http.Header),
		Body:       failedBody,
	}})
	reader, err := service.GetRangeReader(context.Background(), "object", 0, 10)
	if reader != nil || err == nil {
		t.Fatalf("GetRangeReader returned reader=%v err=%v, want failed response error", reader, err)
	}
	var responseErr *apierror.APIError
	if !errors.As(err, &responseErr) || responseErr.Status != http.StatusInternalServerError {
		t.Fatalf("GetRangeReader error = %T %v, want typed 500 response error", err, err)
	}
	if !failedBody.closed {
		t.Fatal("failed range response body was not closed")
	}

	fullBody := &closeTrackingReader{reader: strings.NewReader("full")}
	requester := service.httpClient.(*downloadResponseRequester)
	requester.response = &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: fullBody}
	reader, err = service.GetRangeReader(context.Background(), "object", 0, 10)
	if reader != nil || !errors.Is(err, transfer.ErrRangeIgnored) || !fullBody.closed {
		t.Fatalf("200 range fallback returned reader=%v err=%v closed=%v", reader, err, fullBody.closed)
	}

	partialBody := &closeTrackingReader{reader: strings.NewReader("part")}
	partialHeaders := make(http.Header)
	partialHeaders.Set("Content-Range", "bytes 0-3/4")
	requester.response = &http.Response{StatusCode: http.StatusPartialContent, Header: partialHeaders, Body: partialBody}
	reader, err = service.GetRangeReader(context.Background(), "object", 0, 4)
	if err != nil || reader == nil {
		t.Fatalf("206 range response returned reader=%v err=%v", reader, err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(data) != "part" || !partialBody.closed {
		t.Fatalf("206 range body data=%q readErr=%v closeErr=%v closed=%v", data, readErr, closeErr, partialBody.closed)
	}
}

type scopedRequestTransport struct {
	calls    int
	requests []*http.Request
}

func (t *scopedRequestTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	t.requests = append(t.requests, req)
	var payload []byte
	if strings.HasSuffix(req.URL.Path, "/data/upload/guid") {
		payload = []byte(`{"url":"https://upload.example/object"}`)
	} else if req.URL.Path == "/data/multipart/init" {
		payload = []byte(`{"upload_id":"upload-id","guid":"guid"}`)
	} else {
		return nil, errors.New("unexpected generated request: " + req.URL.Path)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(string(payload))),
		Request:    req,
	}, nil
}

func newScopedService(t *testing.T, transport *scopedRequestTransport) *DataService {
	t.Helper()
	client, err := internalapi.NewClientWithResponses("https://api.example", internalapi.WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatalf("NewClientWithResponses returned error: %v", err)
	}
	return NewDataService(client, &downloadResponseRequester{response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}}, discardLogger(), nil)
}

func TestUploadScopeSelectionRejectsAmbiguousMetadataBeforeRequests(t *testing.T) {
	for _, test := range []struct {
		name string
		data map[string][]string
	}{
		{name: "multiple organizations", data: map[string][]string{"org-a": {"project-a"}, "org-b": {"project-b"}}},
		{name: "multiple projects", data: map[string][]string{"org-a": {"project-a", "project-b"}}},
		{name: "mixed broad and project", data: map[string][]string{"org-a": {"project-a", " "}}},
		{name: "multiple broad organizations", data: map[string][]string{"org-a": {}, "org-b": {}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &scopedRequestTransport{}
			service := newScopedService(t, transport)
			metadata := common.FileMetadata{Authorizations: test.data}
			if _, err := service.ResolveUploadURL(context.Background(), "guid", "file", metadata, "bucket"); !errors.Is(err, errorapi.ErrInvalidInput) {
				t.Fatalf("ResolveUploadURL error = %v, want invalid input", err)
			}
			if _, _, err := service.InitMultipartUploadWithMetadata(context.Background(), "guid", "file", "bucket", metadata); !errors.Is(err, errorapi.ErrInvalidInput) {
				t.Fatalf("InitMultipartUploadWithMetadata error = %v, want invalid input", err)
			}
			if transport.calls != 0 {
				t.Fatalf("generated request count = %d, want 0", transport.calls)
			}
		})
	}
}

func TestUploadScopeSelectionAcceptsSingleNormalizedScopes(t *testing.T) {
	tests := []struct {
		name        string
		metadata    common.FileMetadata
		wantOrg     string
		wantProject string
	}{
		{name: "unscoped", metadata: common.FileMetadata{}},
		{name: "project", metadata: common.FileMetadata{Authorizations: map[string][]string{" org-a ": {" project-a ", "project-a"}}}, wantOrg: "org-a", wantProject: "project-a"},
		{name: "organization", metadata: common.FileMetadata{Authorizations: map[string][]string{" org-a ": {" ", ""}}}, wantOrg: "org-a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := &scopedRequestTransport{}
			service := newScopedService(t, transport)
			metadata := test.metadata
			if _, err := service.ResolveUploadURL(context.Background(), "guid", "file", metadata, "bucket"); err != nil {
				t.Fatalf("ResolveUploadURL error = %v", err)
			}
			if len(transport.requests) != 1 {
				t.Fatalf("generated request count = %d, want 1", len(transport.requests))
			}
			query := transport.requests[0].URL.Query()
			if query.Get("organization") != test.wantOrg || query.Get("project") != test.wantProject {
				t.Fatalf("upload scope query = %v, want organization=%q project=%q", query, test.wantOrg, test.wantProject)
			}
		})
	}
}

func TestUploadScopeFromMetadataTrimsAndDeduplicatesPairs(t *testing.T) {
	org, project, err := uploadScopeFromMetadata(common.FileMetadata{Authorizations: map[string][]string{
		" org ": {" project ", "project"},
	}})
	if err != nil || org != "org" || project != "project" {
		t.Fatalf("uploadScopeFromMetadata() = (%q, %q, %v), want (org, project, nil)", org, project, err)
	}
	if _, _, err := uploadScopeFromMetadata(common.FileMetadata{Authorizations: map[string][]string{"org": {"project", "other"}}}); !errors.Is(err, errorapi.ErrInvalidInput) {
		t.Fatalf("multiple project scopes error = %v, want invalid input", err)
	}
}
