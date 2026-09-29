package services

import (
	"context"
	"net/http"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/drs"
)

type replaceObjectClientStub struct {
	drsapi.ClientWithResponsesInterface
	objectID drsapi.ObjectId
	body     drsapi.ReplaceObjectJSONRequestBody
	response drsapi.DrsObject
}

func (c *replaceObjectClientStub) ReplaceObjectWithResponse(_ context.Context, objectID drsapi.ObjectId, body drsapi.ReplaceObjectJSONRequestBody, _ ...drsapi.RequestEditorFn) (*drsapi.ReplaceObjectResponse, error) {
	c.objectID = objectID
	c.body = body
	return &drsapi.ReplaceObjectResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK, Status: "200 OK"},
		JSON200:      &c.response,
	}, nil
}

func TestDRSServiceReplaceObjectPassesPreconditionAndCandidate(t *testing.T) {
	response := drsapi.DrsObject{Id: "did-1"}
	client := &replaceObjectClientStub{response: response}
	service := NewDRSService(client)
	candidate := drsapi.DrsObjectCandidate{Checksums: []drsapi.Checksum{{Type: "sha256", Checksum: "new-sha"}}}

	got, err := service.ReplaceObject(context.Background(), "did-1", "old-sha", candidate)
	if err != nil {
		t.Fatalf("ReplaceObject() error = %v", err)
	}
	if got.Id != response.Id || client.objectID != "did-1" {
		t.Fatalf("result id = %q, request id = %q", got.Id, client.objectID)
	}
	if client.body.ExpectedOldSha256 != "old-sha" || client.body.Candidate.Checksums[0].Checksum != "new-sha" {
		t.Fatalf("replacement body = %+v", client.body)
	}
}
