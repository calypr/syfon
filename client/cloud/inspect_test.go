package cloud

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestInspectObjectErrorsRedactProviderURLCredentials(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		secrets []string
	}{
		{
			name:    "AWS",
			url:     "s3://bucket/path/%zz?X-Amz-Credential=aws-credential&X-Amz-Signature=aws-signature",
			secrets: []string{"aws-credential", "aws-signature"},
		},
		{
			name:    "GCS",
			url:     "gs:///path/to/object?X-Goog-Credential=gcs-credential&X-Goog-Signature=gcs-signature",
			secrets: []string{"gcs-credential", "gcs-signature"},
		},
		{
			name:    "Azure",
			url:     "https://account.blob.core.windows.net/container?sv=azure-version&sig=azure-signature",
			secrets: []string{"azure-version", "azure-signature"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := InspectObject(context.Background(), ObjectParameters{ObjectURL: test.url})
			if err == nil {
				t.Fatal("expected malformed signed URL to fail")
			}
			for _, secret := range test.secrets {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("error exposed signed URL credential %q: %v", secret, err)
				}
			}
			if strings.Contains(err.Error(), "?") {
				t.Fatalf("error exposed signed URL query: %v", err)
			}
		})
	}
}

func TestSanitizeCloudErrorRedactsProviderURLsAndPreservesCause(t *testing.T) {
	for _, test := range []struct {
		name  string
		url   string
		token string
	}{
		{name: "AWS", url: "https://bucket.s3.us-west-2.amazonaws.com/key?X-Amz-Signature=aws-signed-token", token: "aws-signed-token"},
		{name: "GCS", url: "https://storage.googleapis.com/bucket/key?X-Goog-Signature=gcs-signed-token", token: "gcs-signed-token"},
		{name: "Azure", url: "https://account.blob.core.windows.net/container/key?sv=azure-version&sig=azure-signed-token", token: "azure-signed-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cause := errors.New("provider request failed for " + test.url)
			err := sanitizeCloudError(cause)
			if !errors.Is(err, cause) {
				t.Fatal("sanitized error did not preserve its cause")
			}
			if strings.Contains(err.Error(), test.token) || strings.Contains(err.Error(), "?") {
				t.Fatalf("provider error exposed signed URL credentials: %v", err)
			}
		})
	}
}

func TestParseObjectLocation_S3Scheme(t *testing.T) {
	loc, err := parseObjectLocation("s3://my-bucket/path/to/file.bam", "", ObjectParameters{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if loc.bucket != "my-bucket" || loc.key != "path/to/file.bam" || loc.bucketURL != "s3://my-bucket" {
		t.Fatalf("unexpected location: %+v", loc)
	}
}

func TestParseObjectLocation_GCSAndAzure(t *testing.T) {
	cases := []struct {
		raw       string
		bucket    string
		key       string
		bucketURL string
	}{
		{"gs://my-gcs-bucket/path/to/file.bam", "my-gcs-bucket", "path/to/file.bam", "gs://my-gcs-bucket"},
		{"https://myacct.blob.core.windows.net/mycontainer/path/to/blob.bam", "mycontainer", "path/to/blob.bam", "azblob://mycontainer?account_name=myacct"},
	}
	for _, tc := range cases {
		loc, err := parseObjectLocation(tc.raw, "", ObjectParameters{})
		if err != nil {
			t.Fatalf("parseObjectLocation(%q): %v", tc.raw, err)
		}
		if loc.bucket != tc.bucket || loc.key != tc.key || loc.bucketURL != tc.bucketURL {
			t.Fatalf("unexpected location for %q: %+v", tc.raw, loc)
		}
	}
}

func TestParseObjectLocation_S3UsesPassedRegionAndEndpoint(t *testing.T) {
	loc, err := parseObjectLocation("s3://cbds/path/to/file.bin", "", ObjectParameters{
		S3Region:   "us-east-1",
		S3Endpoint: "https://aced-storage.ohsu.edu/",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if loc.bucketURL == "" {
		t.Fatal("expected bucketURL")
	}
}

func TestNormalizeAndExtractSHA256(t *testing.T) {
	hex := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if got := normalizeSHA256("sha256:" + hex); got != hex {
		t.Fatalf("normalizeSHA256 mismatch: %q", got)
	}
	got := extractSHA256FromMetadata(map[string]string{"checksum-sha256": "sha256:" + hex})
	if got != hex {
		t.Fatalf("extractSHA256FromMetadata mismatch: %q", got)
	}
}
