package services

import (
	"net/url"
	"testing"
)

func TestCanonicalObjectURLPreservesReservedObjectKeyBytes(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		signedURL     string
		wantURL       string
		wantObjectKey string
	}{
		{
			name:          "reserved delimiters and literal percent sequence",
			signedURL:     "https://upload.example/bucket/dir%2Fslash%3Fquery%23fragment%25literal%252F?signature=secret",
			wantURL:       "s3://bucket/dir/slash%3Fquery%23fragment%25literal%252F",
			wantObjectKey: "dir/slash?query#fragment%literal%2F",
		},
		{
			name:          "escaped slash is a slash byte in the object key",
			signedURL:     "https://upload.example/bucket/a%2Fb?signature=secret",
			wantURL:       "s3://bucket/a/b",
			wantObjectKey: "a/b",
		},
		{
			name:          "literal percent slash sequence remains literal",
			signedURL:     "https://upload.example/bucket/a%252Fb?signature=secret",
			wantURL:       "s3://bucket/a%252Fb",
			wantObjectKey: "a%2Fb",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotURL, err := (&DataService{}).CanonicalObjectURL(testCase.signedURL, "bucket", "")
			if err != nil {
				t.Fatalf("CanonicalObjectURL() error = %v", err)
			}
			if gotURL != testCase.wantURL {
				t.Fatalf("CanonicalObjectURL() = %q, want %q", gotURL, testCase.wantURL)
			}

			parsedURL, err := url.Parse(gotURL)
			if err != nil {
				t.Fatalf("url.Parse() error = %v", err)
			}
			if parsedURL.Scheme != "s3" || parsedURL.Host != "bucket" || parsedURL.Path != "/"+testCase.wantObjectKey {
				t.Fatalf("parsed URL = (%q, %q, %q), want (%q, %q, %q)", parsedURL.Scheme, parsedURL.Host, parsedURL.Path, "s3", "bucket", "/"+testCase.wantObjectKey)
			}
		})
	}
}
