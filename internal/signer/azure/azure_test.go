package azure

import (
	"encoding/base64"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

func TestAzureSASProtocol(t *testing.T) {
	if got := azureSASProtocol("http://localhost:10000/devstoreaccount1"); got != sas.ProtocolHTTPSandHTTP {
		t.Fatalf("expected HTTP endpoint to allow HTTP+HTTPS, got %v", got)
	}
	if got := azureSASProtocol("https://acct.blob.core.windows.net"); got != sas.ProtocolHTTPS {
		t.Fatalf("expected HTTPS endpoint to require HTTPS, got %v", got)
	}
	if got := azureSASProtocol("://bad-url"); got != sas.ProtocolHTTPS {
		t.Fatalf("expected invalid endpoint fallback to HTTPS, got %v", got)
	}
}

func TestAzureServiceURLAndAccountHelpers(t *testing.T) {
	s := &AzureSigner{}
	if got := s.azureServiceURL("acct", ""); got != "https://acct.blob.core.windows.net" {
		t.Fatalf("unexpected default azure service url: %s", got)
	}
	if got := s.azureServiceURL("", "localhost:10000/devstoreaccount1"); got != "https://localhost:10000/devstoreaccount1" {
		t.Fatalf("unexpected endpoint-normalized azure service url: %s", got)
	}

	if got := s.azureAccountFromEndpoint("http://localhost:10000/devstoreaccount1"); got != "localhost" {
		t.Fatalf("unexpected parsed account from localhost endpoint: %s", got)
	}
	if got := s.azureAccountFromEndpoint("https://myacct.blob.core.windows.net"); got != "myacct" {
		t.Fatalf("unexpected parsed account from azure endpoint: %s", got)
	}
	if got := s.azureAccountFromEndpoint("not a url"); got != "" {
		t.Fatalf("expected empty account for invalid endpoint, got %q", got)
	}
}

func TestAzureBlobURL_EscapesObjectPath(t *testing.T) {
	s := &AzureSigner{}
	got := s.azureBlobURL("https://acct.blob.core.windows.net", "bucket", "path with spaces/a+b.txt")
	want := "https://acct.blob.core.windows.net/bucket/path%20with%20spaces/a+b.txt"
	if got != want {
		t.Fatalf("unexpected blob URL:\n got: %s\nwant: %s", got, want)
	}
}

func TestAzureMultipartHelpers(t *testing.T) {
	s := &AzureSigner{}
	blockID := s.azureBlockID("upload-abc", 42)
	raw, err := base64.StdEncoding.DecodeString(blockID)
	if err != nil {
		t.Fatalf("failed to decode block ID: %v", err)
	}
	if got := string(raw); got != "upload-abc:00000042" {
		t.Fatalf("unexpected block ID payload: %s", got)
	}
}
