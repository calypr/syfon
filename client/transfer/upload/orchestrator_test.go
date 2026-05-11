package upload

import (
	"context"
	"fmt"
	"testing"

	drsapi "github.com/calypr/syfon/apigen/client/drs"
)

type metadataClientStub struct {
	registeredID string
	registers    int
	requests     []drsapi.RegisterObjectsJSONRequestBody
	object       drsapi.DrsObject
	getErr       error
}

func (m *metadataClientStub) GetObject(context.Context, string) (drsapi.DrsObject, error) {
	if m.getErr != nil {
		return drsapi.DrsObject{}, m.getErr
	}
	if m.object.Id != "" {
		return m.object, nil
	}
	return drsapi.DrsObject{}, fmt.Errorf("not found")
}

func (m *metadataClientStub) RegisterObjects(_ context.Context, req drsapi.RegisterObjectsJSONRequestBody) (drsapi.N201ObjectsCreated, error) {
	m.registers++
	m.requests = append(m.requests, cloneRegisterRequest(req))
	return drsapi.N201ObjectsCreated{
		Objects: []drsapi.DrsObject{{
			Id: m.registeredID,
		}},
	}, nil
}

func cloneRegisterRequest(req drsapi.RegisterObjectsJSONRequestBody) drsapi.RegisterObjectsJSONRequestBody {
	out := req
	out.Candidates = append([]drsapi.DrsObjectCandidate(nil), req.Candidates...)
	for i := range out.Candidates {
		if out.Candidates[i].ControlledAccess != nil {
			values := append([]string(nil), (*out.Candidates[i].ControlledAccess)...)
			out.Candidates[i].ControlledAccess = &values
		}
		if out.Candidates[i].AccessMethods != nil {
			values := append([]drsapi.AccessMethod(nil), (*out.Candidates[i].AccessMethods)...)
			out.Candidates[i].AccessMethods = &values
		}
	}
	return out
}

func TestRegisterFileUploadsUsingRegisteredObjectID(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()

	uploader := &uploaderStub{}
	metadata := &metadataClientStub{registeredID: "server-object-id"}
	name := "payload.bin"
	obj := &drsapi.DrsObject{
		Id:   "requested-object-id",
		Name: &name,
		Size: 7,
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: "3d71f043937a09b77826109db4f2b47c46f19923ef823f6a777a15fde0b2c9c7",
		}},
	}

	if _, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "bucket-a"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if uploader.lastResolve.guid != "server-object-id" {
		t.Fatalf("expected upload URL to use registered object id, got %q", uploader.lastResolve.guid)
	}
	if uploader.lastResolve.fileName != "3d71f043937a09b77826109db4f2b47c46f19923ef823f6a777a15fde0b2c9c7" {
		t.Fatalf("expected checksum upload key, got %q", uploader.lastResolve.fileName)
	}
}

func TestRegisterFilePreservesScopedRoutingMetadata(t *testing.T) {
	t.Parallel()

	file := createTempFileWithData(t, "payload")
	defer file.Close()

	name := "payload.bin"
	controlledAccess := []string{"/organization/syfon/project/e2e"}
	accessMethods := []drsapi.AccessMethod{{
		Type: "s3",
		AccessUrl: &struct {
			Headers *[]string `json:"headers,omitempty"`
			Url     string    `json:"url"`
		}{Url: "s3://syfon-e2e-bucket/project-subpath/3d71f043937a09b77826109db4f2b47c46f19923ef823f6a777a15fde0b2c9c7"},
	}}
	obj := &drsapi.DrsObject{
		Id:               "requested-object-id",
		Name:             &name,
		Size:             7,
		ControlledAccess: &controlledAccess,
		AccessMethods:    &accessMethods,
		Checksums: []drsapi.Checksum{{
			Type:     "sha256",
			Checksum: "3d71f043937a09b77826109db4f2b47c46f19923ef823f6a777a15fde0b2c9c7",
		}},
	}

	uploader := &uploaderStub{}
	metadata := &metadataClientStub{
		registeredID: "server-object-id",
		object: drsapi.DrsObject{
			Id:               "server-object-id",
			Name:             &name,
			Size:             7,
			ControlledAccess: &controlledAccess,
			AccessMethods:    &accessMethods,
			Checksums:        obj.Checksums,
		},
	}

	if _, err := RegisterFile(context.Background(), uploader, metadata, obj, file.Name(), "syfon-e2e-bucket"); err != nil {
		t.Fatalf("RegisterFile returned error: %v", err)
	}
	if len(metadata.requests) != 2 {
		t.Fatalf("expected initial and final register calls, got %d", len(metadata.requests))
	}
	for i, req := range metadata.requests {
		if len(req.Candidates) != 1 {
			t.Fatalf("register call %d expected one candidate, got %d", i, len(req.Candidates))
		}
		candidate := req.Candidates[0]
		if candidate.ControlledAccess == nil || len(*candidate.ControlledAccess) != 1 || (*candidate.ControlledAccess)[0] != controlledAccess[0] {
			t.Fatalf("register call %d did not preserve controlled_access: %#v", i, candidate.ControlledAccess)
		}
		if candidate.AccessMethods == nil || len(*candidate.AccessMethods) == 0 {
			t.Fatalf("register call %d did not preserve access methods: %#v", i, candidate.AccessMethods)
		}
	}
	if obj.ControlledAccess == nil || len(*obj.ControlledAccess) != 1 || (*obj.ControlledAccess)[0] != controlledAccess[0] {
		t.Fatalf("returned object did not preserve controlled_access: %#v", obj.ControlledAccess)
	}
}
