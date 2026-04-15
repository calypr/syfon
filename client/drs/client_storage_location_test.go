package drs

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/pkg/request"
)

type storageLocationRequestor struct {
	url string
}

func (r *storageLocationRequestor) New(method, path string) *request.RequestBuilder {
	return &request.RequestBuilder{Method: method, Url: path, Headers: map[string]string{}}
}

func (r *storageLocationRequestor) Do(ctx context.Context, rb *request.RequestBuilder) (*http.Response, error) {
	_ = ctx
	_ = rb
	rec := internalapi.InternalRecordResponse{Did: "did-1", Urls: []string{r.url}}
	body, _ := json.Marshal(rec)
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(string(body))),
	}, nil
}

func (r *storageLocationRequestor) DoJSON(ctx context.Context, rb *request.RequestBuilder, out any) error {
	resp, err := r.Do(ctx, rb)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func TestDrsClientGetStorageLocation_S3GCSAzure(t *testing.T) {
	tests := []struct {
		name       string
		accessURL  string
		wantBucket string
		wantKey    string
		wantErr    bool
	}{
		{name: "s3", accessURL: "s3://s3-bucket/path/to/object.bin", wantBucket: "s3-bucket", wantKey: "path/to/object.bin"},
		{name: "gcs", accessURL: "gs://gcs-bucket/path/to/object.bin", wantBucket: "gcs-bucket", wantKey: "path/to/object.bin"},
		{name: "azure", accessURL: "azblob://az-container/path/to/object.bin", wantBucket: "az-container", wantKey: "path/to/object.bin"},
		{name: "invalid missing key", accessURL: "gs://gcs-bucket", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := NewLocalDrsClient(&storageLocationRequestor{url: tc.accessURL}, "http://example.org", nil)
			drsClient, ok := client.(*DrsClient)
			if !ok {
				t.Fatal("expected *DrsClient")
			}
			bucket, key, err := drsClient.GetStorageLocation(context.Background(), "did-1")
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tc.accessURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetStorageLocation returned error: %v", err)
			}
			if bucket != tc.wantBucket || key != tc.wantKey {
				t.Fatalf("unexpected storage location: got (%q,%q) want (%q,%q)", bucket, key, tc.wantBucket, tc.wantKey)
			}
		})
	}
}
