package copyprojectrefs

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/client/services"
)

type recordLookupClient struct {
	internalapi.ClientWithResponsesInterface
	getStatus   int
	getErr      error
	createCalls int
}

func (c *recordLookupClient) InternalGetWithResponse(context.Context, string, ...internalapi.RequestEditorFn) (*internalapi.InternalGetResponse, error) {
	if c.getErr != nil {
		return nil, c.getErr
	}
	return &internalapi.InternalGetResponse{HTTPResponse: &http.Response{StatusCode: c.getStatus}}, nil
}

func (c *recordLookupClient) InternalCreateWithResponse(_ context.Context, record internalapi.InternalCreateJSONRequestBody, _ ...internalapi.RequestEditorFn) (*internalapi.InternalCreateResponse, error) {
	c.createCalls++
	return &internalapi.InternalCreateResponse{HTTPResponse: &http.Response{StatusCode: http.StatusCreated}, JSON201: &internalapi.InternalRecordResponse{Did: record.Did}}, nil
}

func TestUpsertExactRecordCreatesOnlyAfterNotFound(t *testing.T) {
	for _, test := range []struct {
		name        string
		status      int
		lookupError error
		wantCreates int
		wantError   bool
	}{
		{name: "missing", status: http.StatusNotFound, wantCreates: 1},
		{name: "server failure", status: http.StatusInternalServerError, wantError: true},
		{name: "transport failure", lookupError: errors.New("connection lost"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &recordLookupClient{getStatus: test.status, getErr: test.lookupError}
			err := upsertExactRecord(context.Background(), services.NewIndexService(client), internalapi.InternalRecord{Did: "record"})
			if (err != nil) != test.wantError || client.createCalls != test.wantCreates {
				t.Fatalf("upsert error = %v, creates = %d, want error=%t creates=%d", err, client.createCalls, test.wantError, test.wantCreates)
			}
		})
	}
}
