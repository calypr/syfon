package upload

import (
	"context"
	"testing"

	"github.com/calypr/syfon/client/common"
)

func TestGeneratePresignedUploadURL(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stub := &uploaderStub{}
	metadata := common.FileMetadata{Metadata: map[string]any{"k": "v"}}

	signed, err := GeneratePresignedUploadURL(ctx, stub, "object.txt", metadata, "bucket-a")
	if err != nil {
		t.Fatalf("GeneratePresignedUploadURL returned error: %v", err)
	}
	if signed != "https://upload.example/signed" {
		t.Fatalf("unexpected signed URL %q", signed)
	}
	if stub.lastResolve.guid != "" || stub.lastResolve.fileName != "object.txt" || stub.lastResolve.bucket != "bucket-a" {
		t.Fatalf("unexpected resolve args: %+v", stub.lastResolve)
	}
}
