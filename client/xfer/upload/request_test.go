package upload

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/calypr/syfon/client/common"
)

func TestGeneratePresignedUploadURLAndGenerateUploadRequest(t *testing.T) {
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

	file := createTempFileWithData(t, "hello world")
	req := uploadRequest{sourcePath: file.Name(), objectKey: "object.txt", guid: "guid-1", metadata: metadata, bucket: "bucket-a"}
	got, err := generateUploadRequest(ctx, stub, req, file, nil)
	if err != nil {
		t.Fatalf("generateUploadRequest returned error: %v", err)
	}
	if got.presignedURL == "" {
		t.Fatal("expected resolved presigned URL")
	}
}

func TestGenerateUploadRequestErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	file := createTempFileWithData(t, "x")

	resolveErr := &uploaderStub{resolveFunc: func(context.Context, string, string, common.FileMetadata, string) (string, error) {
		return "", errors.New("resolve failed")
	}}
	_, err := generateUploadRequest(ctx, resolveErr, uploadRequest{objectKey: "x", guid: "g"}, file, nil)
	if err == nil || !strings.Contains(err.Error(), "upload error") {
		t.Fatalf("expected wrapped resolve error, got %v", err)
	}

	bigPath := t.TempDir() + "/big.bin"
	createSparseFile(t, bigPath, common.FileSizeLimit+1)
	bigFile, err := os.Open(bigPath)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	defer bigFile.Close()
	_, err = generateUploadRequest(ctx, &uploaderStub{}, uploadRequest{objectKey: "big", guid: "g", presignedURL: "https://upload"}, bigFile, nil)
	if err == nil || !strings.Contains(err.Error(), "file size exceeds limit") {
		t.Fatalf("expected size limit error, got %v", err)
	}
}

