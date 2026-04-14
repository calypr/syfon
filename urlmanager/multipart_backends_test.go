package urlmanager

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
)

func TestNormalizedMultipartParts_SortsByPartNumber(t *testing.T) {
	in := []MultipartPart{
		{PartNumber: 3, ETag: "e3"},
		{PartNumber: 1, ETag: "e1"},
		{PartNumber: 2, ETag: "e2"},
	}
	got := normalizedMultipartParts(in)
	if len(got) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(got))
	}
	if got[0].PartNumber != 1 || got[1].PartNumber != 2 || got[2].PartNumber != 3 {
		t.Fatalf("parts were not sorted: %+v", got)
	}
}

func TestAzureBlockID_RoundTrip(t *testing.T) {
	blockID := azureBlockID("upload-abc", 42)
	raw, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		t.Fatalf("failed to decode block ID: %v", err)
	}
	if got := string(raw); got != "upload-abc:00000042" {
		t.Fatalf("unexpected decoded block ID: %q", got)
	}
}

func TestAzureBlobURL_EscapesObjectPath(t *testing.T) {
	got := azureBlobURL("https://acct.blob.core.windows.net", "bucket", "path with spaces/a+b.txt")
	want := "https://acct.blob.core.windows.net/bucket/path%20with%20spaces/a+b.txt"
	if got != want {
		t.Fatalf("unexpected blob URL:\n got: %s\nwant: %s", got, want)
	}
}

func TestAzureMultipartBackend_SignPart(t *testing.T) {
	accountName := "acct"
	accountKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	shared, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Fatalf("failed to create azure shared key credential: %v", err)
	}

	backend := &azureMultipartBackend{
		m: &Manager{
			signing: config.SigningConfig{DefaultExpirySeconds: 900},
		},
		item: &cacheItem{
			AzureSharedKey:  shared,
			AzureServiceURL: "https://acct.blob.core.windows.net",
		},
	}

	signedURL, err := backend.SignPart(context.Background(), "bucket", "obj/name.bin", "up123", 7)
	if err != nil {
		t.Fatalf("SignPart failed: %v", err)
	}
	parsed, err := url.Parse(signedURL)
	if err != nil {
		t.Fatalf("failed to parse signed URL: %v", err)
	}
	q := parsed.Query()
	if q.Get("comp") != "block" {
		t.Fatalf("expected comp=block, got %q", q.Get("comp"))
	}
	if q.Get("blockid") == "" {
		t.Fatal("expected blockid query parameter")
	}
	if q.Get("sig") == "" {
		t.Fatal("expected SAS signature query parameter (sig)")
	}
}

func TestGCSSignedUploadPartURL_RequiresServiceAccountSigningMaterial(t *testing.T) {
	_, err := gcsSignedUploadPartURL("bucket", "obj", &core.S3Credential{
		AccessKey: "",
		SecretKey: "",
	}, config.SigningConfig{DefaultExpirySeconds: 900})
	if err == nil {
		t.Fatal("expected error for missing gcs signing credentials")
	}
	if !strings.Contains(err.Error(), "gcs multipart signing requires service account credentials") {
		t.Fatalf("unexpected error: %v", err)
	}
}
