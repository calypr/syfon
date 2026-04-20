package common

import (
	"reflect"
	"testing"
)

func TestUniqueStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{"Empty", []string{}, []string{}},
		{"No Duplicates", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"With Duplicates", []string{"a", "b", "a", "c", "b"}, []string{"a", "b", "c"}},
		{"With Empty Strings", []string{"a", "", "b", ""}, []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UniqueStrings(tt.input); !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("UniqueStrings() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestUniqueStringsCaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{"Empty", []string{}, []string{}},
		{"No Duplicates", []string{"a", "b", "c"}, []string{"a", "b", "c"}},
		{"With Duplicates Case", []string{"A", "a", "B", "b"}, []string{"A", "B"}},
		{"With Spacing", []string{" a ", "a", " B"}, []string{" a ", " B"}},
		{"With Empty Strings", []string{" ", ""}, []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := UniqueStringsCaseInsensitive(tt.input); !reflect.DeepEqual(got, tt.expected) {
				t.Errorf("UniqueStringsCaseInsensitive() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestSchemeFromURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"HTTP", "http://example.com", "http"},
		{"HTTPS", "HTTPS://example.com", "https"},
		{"S3", "s3://my-bucket", "s3"},
		{"No Scheme", "example.com", ""},
		{"Empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SchemeFromURL(tt.input); got != tt.expected {
				t.Errorf("SchemeFromURL() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestNormalizeUploadKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		id       string
		expected string
	}{
		{"Normal Key", "my-key", "my-id", "my-key"},
		{"Empty Key", "", "my-id", "my-id"},
		{"Space Key", "  ", "my-id", "my-id"},
		{"Untrimmed Key", " my-key ", "my-id", "my-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NormalizeUploadKey(tt.key, tt.id); got != tt.expected {
				t.Errorf("NormalizeUploadKey() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBucketToURL(t *testing.T) {
	tests := []struct {
		name     string
		bucket   string
		key      string
		expected string
	}{
		{"Normal", "my-bucket", "my-key", "s3://my-bucket/my-key"},
		{"Bucket With Scheme", "s3://my-bucket", "my-key", "s3://my-bucket/my-key"},
		{"Key With Leading Slash", "my-bucket", "/my-key", "s3://my-bucket/my-key"},
		{"Both With Extras", "s3://my-bucket", "/my-key", "s3://my-bucket/my-key"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BucketToURL(tt.bucket, tt.key); got != tt.expected {
				t.Errorf("BucketToURL() = %v, want %v", got, tt.expected)
			}
		})
	}
}
