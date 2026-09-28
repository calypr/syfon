package services

import (
	"context"
	"net/http"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/apigen/internalapi"
)

type statDRSClient struct {
	drsapi.ClientWithResponsesInterface
	status int
	object *drsapi.DrsObject
}

func (c statDRSClient) GetObjectWithResponse(context.Context, drsapi.ObjectId, *drsapi.GetObjectParams, ...drsapi.RequestEditorFn) (*drsapi.GetObjectResponse, error) {
	response := &drsapi.GetObjectResponse{HTTPResponse: &http.Response{StatusCode: c.status}}
	if c.object != nil {
		response.JSON200 = c.object
	}
	return response, nil
}

type statDownloadClient struct {
	internalapi.ClientWithResponsesInterface
	requests int
}

func (c *statDownloadClient) InternalDownloadWithResponse(context.Context, string, *internalapi.InternalDownloadParams, ...internalapi.RequestEditorFn) (*internalapi.InternalDownloadResponse, error) {
	c.requests++
	location := "https://download.example/object"
	return &internalapi.InternalDownloadResponse{HTTPResponse: &http.Response{StatusCode: http.StatusOK}, JSON200: &internalapi.InternalSignedURL{Url: &location}}, nil
}

func TestStatFallsBackOnlyForMissingDRSMetadata(t *testing.T) {
	for _, test := range []struct {
		name         string
		status       int
		wantRequests int
		wantError    bool
	}{
		{name: "missing metadata", status: http.StatusNotFound, wantRequests: 1},
		{name: "server error", status: http.StatusInternalServerError, wantError: true},
		{name: "access denied", status: http.StatusForbidden, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fallback := &statDownloadClient{}
			service := NewDataService(fallback, nil, discardLogger(), NewDRSService(statDRSClient{status: test.status}))
			metadata, err := service.Stat(context.Background(), "object")
			if (err != nil) != test.wantError || fallback.requests != test.wantRequests {
				t.Fatalf("metadata=%+v error=%v fallback requests=%d, want error=%t requests=%d", metadata, err, fallback.requests, test.wantError, test.wantRequests)
			}
			if !test.wantError && (metadata == nil || metadata.Size != 0 || metadata.SizeKnown || metadata.Identity != "") {
				t.Fatalf("legacy fallback metadata=%+v", metadata)
			}
		})
	}
}

func TestStatMarksDRSZeroSizeAsKnown(t *testing.T) {
	service := NewDataService(nil, nil, discardLogger(), NewDRSService(statDRSClient{
		status: http.StatusOK,
		object: &drsapi.DrsObject{Size: 0},
	}))

	metadata, err := service.Stat(context.Background(), "empty-object")
	if err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	if metadata == nil || metadata.Size != 0 || !metadata.SizeKnown {
		t.Fatalf("Stat metadata=%+v, want known zero size", metadata)
	}
}
