package services

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/calypr/syfon/client/transfer"
)

func newMultipartPartURLTestService(t *testing.T, handler http.Handler) *DataService {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return NewDataService(mustInternalClient(t, server.URL), server.Client(), discardLogger(), nil)
}

func TestMultipartPartURLsAcceptsTypedBatchAndRestoresRequestedOrder(t *testing.T) {
	var calls int
	var request multipartPartURLsRequest
	service := newMultipartPartURLTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"parts":[{"partNumber":2,"presigned_url":"url-2","expires_in":900},{"partNumber":1,"presigned_url":"url-1","expires_in":900}]}`))
	}))

	got, err := service.MultipartPartURLs(context.Background(), "guid", "upload", []int32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if !got.BatchSupported || len(got.Parts) != 2 {
		t.Fatalf("MultipartPartURLs() = %+v, want two-part batch", got)
	}
	for i, part := range got.Parts {
		wantNumber := int32(i + 1)
		if part.PartNumber != wantNumber || part.URL != fmt.Sprintf("url-%d", wantNumber) {
			t.Fatalf("batch part %d = %+v", i, part)
		}
		if remaining := time.Until(part.ExpiresAt); remaining <= 14*time.Minute || remaining > 15*time.Minute {
			t.Fatalf("part %d remaining lifetime = %s, want between 14 and 15 minutes", part.PartNumber, remaining)
		}
	}
	if calls != 1 || request.PartNumber != 1 || fmt.Sprint(request.PartNumbers) != fmt.Sprint([]int32{1, 2}) {
		t.Fatalf("batch requests=%d body=%+v", calls, request)
	}
}

func TestMultipartPartURLsReturnsFirstURLForLegacySingletonResponse(t *testing.T) {
	var calls int
	var requests []multipartPartURLsRequest
	service := newMultipartPartURLTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		var request multipartPartURLsRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		requests = append(requests, request)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"presigned_url": fmt.Sprintf("url-%d", request.PartNumber)})
	}))

	got, err := service.MultipartPartURLs(context.Background(), "guid", "upload", []int32{3, 1, 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []transfer.MultipartPartURL{{PartNumber: 3, URL: "url-3"}}
	if got.BatchSupported || fmt.Sprint(got.Parts) != fmt.Sprint(want) || !got.Parts[0].ExpiresAt.IsZero() {
		t.Fatalf("MultipartPartURLs() = %+v, want zero-expiry singleton %v", got, want)
	}
	if calls != 1 || requests[0].PartNumber != 3 || fmt.Sprint(requests[0].PartNumbers) != fmt.Sprint([]int32{3, 1, 2}) {
		t.Fatalf("legacy response requests=%d first=%+v", calls, requests[0])
	}
}

func TestMultipartPartURLsDoesNotFallbackOnServerErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			service := newMultipartPartURLTestService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"message":"signing failed"}}`))
			}))
			if _, err := service.MultipartPartURLs(context.Background(), "guid", "upload", []int32{1, 2}); err == nil {
				t.Fatal("MultipartPartURLs() returned no error")
			}
			if calls != 1 {
				t.Fatalf("request calls = %d, want 1", calls)
			}
		})
	}
}

func TestMultipartPartURLsRejectsMalformedBatchWithoutFallback(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "omitted", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1"}]}`},
		{name: "duplicate", body: `{"parts":[{"partNumber":1,"presigned_url":"url-a"},{"partNumber":1,"presigned_url":"url-b"}]}`},
		{name: "unknown", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1"},{"partNumber":3,"presigned_url":"url-3"}]}`},
		{name: "empty URL", body: `{"parts":[{"partNumber":1,"presigned_url":""},{"partNumber":2,"presigned_url":"url-2"}]}`},
		{name: "empty plural", body: `{"parts":[],"presigned_url":"legacy-url"}`},
		{name: "null plural", body: `{"parts":null,"presigned_url":"legacy-url"}`},
		{name: "negative expiry", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1","expires_in":-1},{"partNumber":2,"presigned_url":"url-2","expires_in":900}]}`},
		{name: "null expiry", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1","expires_in":null},{"partNumber":2,"presigned_url":"url-2","expires_in":900}]}`},
		{name: "string expiry", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1","expires_in":"900"},{"partNumber":2,"presigned_url":"url-2","expires_in":900}]}`},
		{name: "overflow expiry", body: `{"parts":[{"partNumber":1,"presigned_url":"url-1","expires_in":9223372036854775807},{"partNumber":2,"presigned_url":"url-2","expires_in":900}]}`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			calls := 0
			service := newMultipartPartURLTestService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(testCase.body))
			}))
			if _, err := service.MultipartPartURLs(context.Background(), "guid", "upload", []int32{1, 2}); err == nil {
				t.Fatal("MultipartPartURLs() returned no error")
			}
			if calls != 1 {
				t.Fatalf("request calls = %d, want 1", calls)
			}
		})
	}
}

func TestMultipartPartURLsDowngradesBatchWithoutExpiryMetadata(t *testing.T) {
	service := newMultipartPartURLTestService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"parts":[{"partNumber":1,"presigned_url":"url-1"},{"partNumber":2,"presigned_url":"url-2"}]}`))
	}))
	got, err := service.MultipartPartURLs(context.Background(), "guid", "upload", []int32{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if got.BatchSupported || len(got.Parts) != 1 || got.Parts[0].PartNumber != 1 || got.Parts[0].URL != "url-1" {
		t.Fatalf("MultipartPartURLs() = %+v, want first URL only and batch disabled", got)
	}
	if got.Parts[0].ExpiresAt.IsZero() || got.Parts[0].ExpiresAt.After(time.Now()) {
		t.Fatalf("unknown batch expiry deadline = %s, want nonzero already-expired deadline", got.Parts[0].ExpiresAt)
	}
}
