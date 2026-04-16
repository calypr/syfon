package urlmanager

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/calypr/syfon/config"
	"github.com/calypr/syfon/db/core"
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

func TestGCSEndpointObjectURL(t *testing.T) {
	cred := &core.S3Credential{Endpoint: "http://localhost:4443"}

	uploadURL, ok := gcsEndpointObjectURL(cred, "test-bucket", "path/to/file.txt", http.MethodPut)
	if !ok {
		t.Fatal("expected upload endpoint URL")
	}
	up, err := url.Parse(uploadURL)
	if err != nil {
		t.Fatalf("parse upload url: %v", err)
	}
	if got, want := up.Path, "/upload/storage/v1/b/test-bucket/o"; got != want {
		t.Fatalf("unexpected upload path: got %q want %q", got, want)
	}
	if got := up.Query().Get("uploadType"); got != "media" {
		t.Fatalf("expected uploadType=media, got %q", got)
	}
	if got := up.Query().Get("name"); got != "path/to/file.txt" {
		t.Fatalf("expected upload name to preserve object key, got %q", got)
	}

	downloadURL, ok := gcsEndpointObjectURL(cred, "test-bucket", "path/to/file.txt", http.MethodGet)
	if !ok {
		t.Fatal("expected download endpoint URL")
	}
	dl, err := url.Parse(downloadURL)
	if err != nil {
		t.Fatalf("parse download url: %v", err)
	}
	if got, want := dl.Path, "/storage/v1/b/test-bucket/o/path%2Fto%2Ffile.txt"; got != want {
		t.Fatalf("unexpected download path: got %q want %q", got, want)
	}
	if got := dl.Query().Get("alt"); got != "media" {
		t.Fatalf("expected alt=media, got %q", got)
	}
}

func TestGCSEndpointObjectURL_RequiresEndpoint(t *testing.T) {
	if _, ok := gcsEndpointObjectURL(&core.S3Credential{}, "bucket", "obj", http.MethodGet); ok {
		t.Fatal("expected false when endpoint is missing")
	}
}

func TestGCSSignedURL_UsesEndpointWithoutServiceAccountKey(t *testing.T) {
	cred := &core.S3Credential{Endpoint: "http://localhost:4443"}
	signed, err := gcsSignedURL("test-bucket", "nested/file.txt", http.MethodGet, 5*time.Minute, "", cred, config.SigningConfig{DefaultExpirySeconds: 900})
	if err != nil {
		t.Fatalf("gcsSignedURL returned error: %v", err)
	}
	if !strings.Contains(signed, "/storage/v1/b/test-bucket/o/nested%252Ffile.txt") {
		t.Fatalf("unexpected signed endpoint url: %s", signed)
	}
	if !strings.Contains(signed, "alt=media") {
		t.Fatalf("expected media download query in signed endpoint url: %s", signed)
	}
}

func TestAzureServiceURLAndAccountHelpers(t *testing.T) {
	if got := azureServiceURL("acct", ""); got != "https://acct.blob.core.windows.net" {
		t.Fatalf("unexpected default azure service url: %s", got)
	}
	if got := azureServiceURL("", "localhost:10000/devstoreaccount1"); got != "https://localhost:10000/devstoreaccount1" {
		t.Fatalf("unexpected endpoint-normalized azure service url: %s", got)
	}

	if got := azureAccountFromEndpoint("http://localhost:10000/devstoreaccount1"); got != "localhost" {
		t.Fatalf("unexpected parsed account from localhost endpoint: %s", got)
	}
	if got := azureAccountFromEndpoint("https://myacct.blob.core.windows.net"); got != "myacct" {
		t.Fatalf("unexpected parsed account from azure endpoint: %s", got)
	}
	if got := azureAccountFromEndpoint("not a url"); got != "" {
		t.Fatalf("expected empty account for invalid endpoint, got %q", got)
	}
}
