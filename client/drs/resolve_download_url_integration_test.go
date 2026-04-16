package drs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
)

func TestDrsClientResolveDownloadURL_DirectAccessURL_GCSAndAzure(t *testing.T) {
	tests := []struct {
		name      string
		objectID  string
		accessURL string
	}{
		{name: "gcs direct access url", objectID: "did-gcs-1", accessURL: "gs://gcs-bucket/path/to/object.bin"},
		{name: "azure direct access url", objectID: "did-az-1", accessURL: "azblob://az-container/path/to/object.bin"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spy := &requestSpy{}
			spy.doFunc = func(method, reqURL string, body []byte) (*http.Response, error) {
				_ = body
				if method != http.MethodGet {
					t.Fatalf("expected GET request, got %s", method)
				}
				if strings.Contains(reqURL, "/ga4gh/drs/v1/objects/") {
					t.Fatalf("expected direct access URL path without /access lookup, got %s", reqURL)
				}
				if !strings.HasSuffix(reqURL, "/index/"+tc.objectID) {
					t.Fatalf("expected object metadata request to /index/%s, got %s", tc.objectID, reqURL)
				}

				record := internalapi.InternalRecordResponse{
					Did:  tc.objectID,
					Urls: []string{tc.accessURL},
				}
				payload, err := json.Marshal(record)
				if err != nil {
					t.Fatalf("marshal response: %v", err)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(string(payload))),
				}, nil
			}

			client := NewLocalDrsClient(spy, "http://example.org", nil)
			drsClient, ok := client.(*DrsClient)
			if !ok {
				t.Fatal("expected *DrsClient")
			}

			got, err := drsClient.ResolveDownloadURL(context.Background(), tc.objectID, "")
			if err != nil {
				t.Fatalf("ResolveDownloadURL returned error: %v", err)
			}
			if got != tc.accessURL {
				t.Fatalf("unexpected resolved URL: got %q want %q", got, tc.accessURL)
			}
		})
	}
}
