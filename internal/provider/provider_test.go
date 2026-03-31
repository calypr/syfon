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
		{" file ", "", File},
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
	if FromScheme("s3") != S3 || FromScheme("gs") != GCS || FromScheme("azblob") != Azure || FromScheme("file") != File {
		t.Fatalf("unexpected FromScheme mapping")
	}
	if FromScheme("http") != "" {
		t.Fatalf("expected empty mapping for unknown scheme")
	}

	if ToScheme(S3) != "s3" || ToScheme(GCS) != "gs" || ToScheme(Azure) != "azblob" || ToScheme(File) != "file" {
		t.Fatalf("unexpected ToScheme mapping")
	}
	if ToScheme("unknown") != "s3" {
		t.Fatalf("unknown provider should default to s3 scheme")
	}
}
