package client

import "testing"

func TestDataServiceCanonicalObjectURL_GCSAndAzure(t *testing.T) {
	d := &DataService{}
	tests := []struct {
		name       string
		signedURL  string
		bucketHint string
		fallback   string
		want       string
	}{
		{
			name:       "gcs https signed url with bucket hint",
			signedURL:  "https://storage.googleapis.com/gcs-bucket/path/to/object.bin?X-Goog-Signature=abc",
			bucketHint: "gcs-bucket",
			fallback:   "did:1",
			want:       "s3://gcs-bucket/path/to/object.bin",
		},
		{
			name:       "azure https signed url with bucket hint",
			signedURL:  "https://acct.blob.core.windows.net/az-container/path/to/object.bin?sig=abc",
			bucketHint: "az-container",
			fallback:   "did:2",
			want:       "s3://az-container/path/to/object.bin",
		},
		{
			name:       "gcs scheme preserved",
			signedURL:  "gs://gcs-bucket/path/to/object.bin",
			bucketHint: "",
			fallback:   "did:3",
			want:       "gs://gcs-bucket/path/to/object.bin",
		},
		{
			name:       "azure scheme preserved",
			signedURL:  "azblob://az-container/path/to/object.bin",
			bucketHint: "",
			fallback:   "did:4",
			want:       "azblob://az-container/path/to/object.bin",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := d.CanonicalObjectURL(tc.signedURL, tc.bucketHint, tc.fallback)
			if err != nil {
				t.Fatalf("CanonicalObjectURL returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected canonical URL: got %q want %q", got, tc.want)
			}
		})
	}
}
