package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/calypr/syfon/apigen/errorapi"
	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/common"
	"github.com/calypr/syfon/client/services"
	clienttransfer "github.com/calypr/syfon/client/transfer"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/gofiber/fiber/v3"
)

type multipartTestClient struct{ app *fiber.App }

func (c multipartTestClient) Do(req *http.Request) (*http.Response, error) { return c.app.Test(req) }

type multipartTestScope struct{ prefix string }

func (s *multipartTestScope) LookupBucketScope(_ context.Context, _, project string) (buckets.Scope, bool, error) {
	if project == "" {
		return buckets.Scope{}, false, nil
	}
	return buckets.Scope{Bucket: "physical-bucket", PathPrefix: s.prefix}, true, nil
}

func TestMultipartCompletionReturnsOriginalScopedLocationThroughClient(t *testing.T) {
	provider := &lfsTestStorage{}
	scope := &multipartTestScope{prefix: "original-prefix"}
	objectStore := newDRSObjectStore(t, nil)
	service := transfers.NewService(transfers.Dependencies{
		Objects:           objects.NewService(objectStore),
		Storage:           provider,
		Scopes:            scope,
		MultipartSessions: objectStore.Store,
	})
	app := fiber.New()
	internalapi.RegisterHandlers(app, &internalServer{transfers: service})
	gen, err := internalapi.NewClientWithResponses("http://syfon.test", internalapi.WithHTTPClient(multipartTestClient{app}))
	if err != nil {
		t.Fatal(err)
	}
	client := services.NewDataService(gen, nil, nil, nil)
	uploadID, _, err := client.InitMultipartUploadWithMetadata(context.Background(), "requested-id", "payload.bin", "physical-bucket", common.FileMetadata{Authorizations: map[string][]string{"org": {"project"}}})
	if err != nil {
		t.Fatal(err)
	}
	scope.prefix = "changed-prefix"
	location, err := client.MultipartCompleteWithLocation(context.Background(), "payload.bin", uploadID, []clienttransfer.MultipartPart{{PartNumber: 1, ETag: "etag"}})
	if err != nil {
		t.Fatal(err)
	}
	if location != "s3://physical-bucket/original-prefix/requested-id" {
		t.Fatalf("completed location = %q", location)
	}
	if provider.complete.Target.Key != "original-prefix/requested-id" {
		t.Fatalf("provider completed key = %q", provider.complete.Target.Key)
	}
}

func TestMultipartRoutesRejectInvalidParts(t *testing.T) {
	app := fiber.New()
	internalapi.RegisterHandlers(app, &internalServer{transfers: transfers.NewService(transfers.Dependencies{})})
	tests := []struct {
		name string
		path string
		body any
	}{
		{name: "sign zero", path: "/data/multipart/upload", body: internalapi.InternalMultipartUploadRequest{UploadId: "missing", PartNumber: 0}},
		{name: "sign empty batch", path: "/data/multipart/upload", body: internalapi.InternalMultipartUploadRequest{UploadId: "missing", PartNumbers: func() *[]int32 { values := []int32{}; return &values }()}},
		{name: "complete empty", path: "/data/multipart/complete", body: internalapi.InternalMultipartCompleteRequest{UploadId: "missing", Parts: []internalapi.InternalMultipartPart{}}},
		{name: "complete duplicate", path: "/data/multipart/complete", body: internalapi.InternalMultipartCompleteRequest{UploadId: "missing", Parts: []internalapi.InternalMultipartPart{{PartNumber: 1}, {PartNumber: 1}}}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body, err := json.Marshal(testCase.body)
			if err != nil {
				t.Fatal(err)
			}
			response, err := app.Test(httptest.NewRequest(http.MethodPost, testCase.path, bytes.NewReader(body)))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			payload, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusBadRequest || !bytes.Contains(payload, []byte(errorapi.ErrorCodeInvalidInput)) {
				t.Fatalf("status=%d body=%s", response.StatusCode, payload)
			}
		})
	}
}

func TestMultipartUploadBatchReturnsPartURLsThroughClient(t *testing.T) {
	provider := &lfsTestStorage{partLocation: "https://storage.example/part"}
	scope := &multipartTestScope{prefix: "batch-prefix"}
	objectStore := newDRSObjectStore(t, nil)
	service := transfers.NewService(transfers.Dependencies{
		Objects:           objects.NewService(objectStore),
		Storage:           provider,
		Scopes:            scope,
		MultipartSessions: objectStore.Store,
	})
	app := fiber.New()
	internalapi.RegisterHandlers(app, &internalServer{transfers: service})
	gen, err := internalapi.NewClientWithResponses("http://syfon.test", internalapi.WithHTTPClient(multipartTestClient{app}))
	if err != nil {
		t.Fatal(err)
	}
	client := services.NewDataService(gen, nil, nil, nil)
	uploadID, _, err := client.InitMultipartUploadWithMetadata(context.Background(), "requested-id", "payload.bin", "physical-bucket", common.FileMetadata{Authorizations: map[string][]string{"org": {"project"}}})
	if err != nil {
		t.Fatal(err)
	}
	partNumbers := []int32{2, 1}
	response, err := gen.InternalMultipartUploadWithResponse(context.Background(), internalapi.InternalMultipartUploadJSONRequestBody(internalapi.InternalMultipartUploadRequest{
		Key: "payload.bin", UploadId: uploadID, PartNumber: partNumbers[0], PartNumbers: &partNumbers,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if response.JSON200 == nil || response.JSON200.Parts == nil {
		t.Fatalf("multipart batch response = %+v", response)
	}
	parts := *response.JSON200.Parts
	if len(parts) != len(partNumbers) {
		t.Fatalf("multipart batch returned %d parts, want %d", len(parts), len(partNumbers))
	}
	for i, part := range parts {
		if part.PartNumber != partNumbers[i] || part.PresignedUrl != provider.partLocation || part.ExpiresIn != 900 {
			t.Fatalf("multipart part %d = %+v, want configured 900 second expiry", i, part)
		}
	}
	if provider.partRequest.PartNumber != 1 {
		t.Fatalf("provider last part number = %d, want 1", provider.partRequest.PartNumber)
	}
}

func TestInternalUploadBulkRejectsOverLimitBeforeSigning(t *testing.T) {
	app := fiber.New()
	RegisterRoutes(app, Dependencies{Transfers: transfers.NewService(transfers.Dependencies{})}, Options{Internal: true, MaxBulkRequestLength: 1})
	request := httptest.NewRequest(http.MethodPost, "/data/upload/bulk", strings.NewReader(`{"requests":[{"file_id":"a"},{"file_id":"b"}]}`))
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("bulk upload status = %d, want 413", response.StatusCode)
	}
}
