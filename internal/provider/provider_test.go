package provider

import "testing"

func TestNormalize(t *testing.T) {
	tests := []struct {
		in       string
		fallback string
		want     string
	}{
		{"s3", "", S3},
		{"GS", "", GCS},
		{"azblob", "", Azure},
		{"", "gcs", GCS},
		{"unknown", "", S3},
	}

	for _, tt := range tests {
		got := Normalize(tt.in, tt.fallback)
		if got != tt.want {
			t.Fatalf("Normalize(%q,%q)=%q want %q", tt.in, tt.fallback, got, tt.want)
		}
	}
}

func TestSchemeMappings(t *testing.T) {
	if FromScheme("s3") != S3 || FromScheme("gs") != GCS || FromScheme("azblob") != Azure {
		t.Fatalf("unexpected FromScheme mapping")
	}
	if FromScheme("http") != "" {
		t.Fatalf("expected empty mapping for unknown scheme")
	}

	if ToScheme(S3) != "s3" || ToScheme(GCS) != "gs" || ToScheme(Azure) != "azblob" {
		t.Fatalf("unexpected ToScheme mapping")
	}
	if ToScheme("unknown") != "s3" {
		t.Fatalf("unknown provider should default to s3 scheme")
	}
}

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		bucket   string
		wantErr  bool
	}{
		{name: "s3 valid", provider: S3, bucket: "my-bucket", wantErr: false},
		{name: "s3 invalid dot", provider: S3, bucket: "my.bucket", wantErr: true},
		{name: "gcs valid dotted", provider: GCS, bucket: "my.bucket.example", wantErr: false},
		{name: "gcs valid underscore", provider: GCS, bucket: "my_bucket", wantErr: false},
		{name: "gcs invalid ip", provider: GCS, bucket: "192.168.1.1", wantErr: true},
		{name: "azure valid", provider: Azure, bucket: "my-azure-bucket", wantErr: false},
		{name: "azure invalid dot", provider: Azure, bucket: "my.azure.bucket", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.provider, tt.bucket)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error for provider=%q bucket=%q", tt.provider, tt.bucket)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error for provider=%q bucket=%q: %v", tt.provider, tt.bucket, err)
			}
		})
	}
}

func TestParseBucketProvider(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{in: "", want: S3, ok: true},
		{in: "s3", want: S3, ok: true},
		{in: "gs", want: GCS, ok: true},
		{in: "azure", want: Azure, ok: true},
		{in: "file", ok: false},
	}

	for _, tt := range tests {
		got, err := ParseBucketProvider(tt.in)
		if tt.ok {
			if err != nil {
				t.Fatalf("ParseBucketProvider(%q) unexpected error: %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("ParseBucketProvider(%q)=%q want %q", tt.in, got, tt.want)
			}
			continue
		}
		if err == nil {
			t.Fatalf("ParseBucketProvider(%q) expected error", tt.in)
		}
	}
}
