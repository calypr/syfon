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
}

func (m *metadataClientStub) GetObject(context.Context, string) (drsapi.DrsObject, error) {
	return drsapi.DrsObject{}, fmt.Errorf("not found")
}

func (m *metadataClientStub) RegisterObjects(context.Context, drsapi.RegisterObjectsJSONRequestBody) (drsapi.N201ObjectsCreated, error) {
	m.registers++
	return drsapi.N201ObjectsCreated{
		Objects: []drsapi.DrsObject{{
			Id: m.registeredID,
		}},
	}, nil
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
