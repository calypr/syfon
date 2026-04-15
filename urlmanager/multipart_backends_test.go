package urlmanager

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/db/sqlite"
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

func TestAzureMultipartBackend_InitAndSignPart(t *testing.T) {
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

	uploadID, err := backend.Init(context.Background(), "bucket", "obj/name.bin")
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if strings.TrimSpace(uploadID) == "" {
		t.Fatal("expected non-empty upload id")
	}

	signedURL, err := backend.SignPart(context.Background(), "bucket", "obj/name.bin", uploadID, 7)
	if err != nil {
		t.Fatalf("SignPart failed: %v", err)
	}
	parsed, err := url.Parse(signedURL)
	if err != nil {
		t.Fatalf("failed to parse signed URL: %v", err)
	}
	q := parsed.Query()
	if parsed.EscapedPath() != "/bucket/obj/name.bin" {
		t.Fatalf("unexpected signed path: %s", parsed.EscapedPath())
	}
	if q.Get("comp") != "block" {
		t.Fatalf("expected comp=block, got %q", q.Get("comp"))
	}
	if q.Get("blockid") == "" {
		t.Fatal("expected blockid query parameter")
	}
	if q.Get("sig") == "" {
		t.Fatal("expected SAS signature query parameter (sig)")
	}
	rawBlockID, err := base64.StdEncoding.DecodeString(q.Get("blockid"))
	if err != nil {
		t.Fatalf("failed to decode blockid: %v", err)
	}
	if got, want := string(rawBlockID), uploadID+":00000007"; got != want {
		t.Fatalf("unexpected decoded blockid: got %q want %q", got, want)
	}
}

func TestGCSMultipartBackend_InitAndSignPart(t *testing.T) {
	t.Setenv(core.CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	ctx := context.Background()
	database, err := sqlite.NewSqliteDB(":memory:")
	if err != nil {
		t.Fatalf("failed to init db: %v", err)
	}

	serviceAccountJSON := mustGCSServiceAccountJSON(t, "test-signer@example.iam.gserviceaccount.com")
	if err := database.SaveS3Credential(ctx, &core.S3Credential{
		Bucket:    "gcs-multipart-bucket",
		Provider:  "gcs",
		SecretKey: serviceAccountJSON,
	}); err != nil {
		t.Fatalf("failed to save credential: %v", err)
	}

	backend := &gcsMultipartBackend{
		m:    NewManager(database, config.SigningConfig{DefaultExpirySeconds: 900}),
		item: &cacheItem{},
	}

	uploadID, err := backend.Init(ctx, "gcs-multipart-bucket", "nested/object.bin")
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	if strings.TrimSpace(uploadID) == "" {
		t.Fatal("expected non-empty upload id")
	}

	partURL, err := backend.SignPart(ctx, "gcs-multipart-bucket", "nested/object.bin", uploadID, 3)
	if err != nil {
		t.Fatalf("SignPart failed: %v", err)
	}

	parsed, err := url.Parse(partURL)
	if err != nil {
		t.Fatalf("failed to parse signed URL: %v", err)
	}
	q := parsed.Query()
	if got, want := parsed.EscapedPath(), "/gcs-multipart-bucket/"+multipartPartObjectKey("nested/object.bin", uploadID, 3); got != want {
		t.Fatalf("unexpected signed path: got %q want %q", got, want)
	}
	if got := q.Get("X-Goog-Algorithm"); got != "GOOG4-RSA-SHA256" {
		t.Fatalf("unexpected signing algorithm: %q", got)
	}
	expires, err := strconv.Atoi(q.Get("X-Goog-Expires"))
	if err != nil {
		t.Fatalf("invalid X-Goog-Expires value %q: %v", q.Get("X-Goog-Expires"), err)
	}
	if expires <= 0 || expires > 900 {
		t.Fatalf("unexpected expiry seconds: %d", expires)
	}
	if got := q.Get("X-Goog-Credential"); !strings.Contains(got, "test-signer@example.iam.gserviceaccount.com/") {
		t.Fatalf("unexpected credential scope: %q", got)
	}
	if q.Get("X-Goog-Signature") == "" {
		t.Fatal("expected X-Goog-Signature query parameter")
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

func mustGCSServiceAccountJSON(t *testing.T, clientEmail string) string {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}
	payload, err := json.Marshal(gcsServiceAccountKey{
		ClientEmail: clientEmail,
		PrivateKey: string(pem.EncodeToMemory(&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
		})),
	})
	if err != nil {
		t.Fatalf("failed to marshal service account JSON: %v", err)
	}
	return string(payload)
}
