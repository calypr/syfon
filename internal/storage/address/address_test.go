package address

import "testing"

func TestProviderAliasesAndSchemes(t *testing.T) {
	tests := []struct {
		input, fallback, want string
	}{
		{"s3", "", S3Provider},
		{"GS", "", GCSProvider},
		{"azblob", "", AzureProvider},
		{"", "gcs", GCSProvider},
		{"unknown", "", S3Provider},
	}
	for _, tc := range tests {
		if got := NormalizeProvider(tc.input, tc.fallback); got != tc.want {
			t.Fatalf("NormalizeProvider(%q,%q)=%q want %q", tc.input, tc.fallback, got, tc.want)
		}
	}
	if ProviderFromScheme("s3://") != S3Provider || ProviderFromScheme("az") != AzureProvider || ProviderFromScheme("gcs") != GCSProvider || ProviderFromScheme("file") != FileProvider {
		t.Fatal("provider scheme aliases did not normalize")
	}
	if ProviderToScheme(GCSProvider) != "gs" || ProviderToScheme(AzureProvider) != "azblob" || ProviderToScheme(FileProvider) != "file" || ProviderToScheme("unsupported") != "s3" {
		t.Fatal("provider scheme output did not normalize")
	}
}

func TestProviderValidation(t *testing.T) {
	cases := []struct {
		provider, bucket, endpoint string
		wantErr                    bool
	}{
		{S3Provider, "my-bucket", "", false},
		{S3Provider, "my.bucket", "", true},
		{S3Provider, "EllrottLab", "https://rgw.example", false},
		{S3Provider, "bad/name", "https://rgw.example", true},
		{GCSProvider, "my.bucket.example", "", false},
		{GCSProvider, "my_bucket", "", false},
		{GCSProvider, "goog-bucket", "", true},
		{GCSProvider, "192.168.1.1", "", true},
		{AzureProvider, "my-azure-bucket", "", false},
		{AzureProvider, "my.azure.bucket", "", true},
		{FileProvider, "/tmp/storage-root", "", false},
	}
	for _, tc := range cases {
		err := ValidateBucketNameWithEndpoint(tc.provider, tc.bucket, tc.endpoint)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ValidateBucketNameWithEndpoint(%q,%q,%q) error=%v wantErr=%v", tc.provider, tc.bucket, tc.endpoint, err, tc.wantErr)
		}
	}
}

func TestParseBucketProvider(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", S3Provider}, {"s3", S3Provider}, {"gs", GCSProvider}, {"gcs", GCSProvider},
		{"azure", AzureProvider}, {"azblob", AzureProvider}, {"file", FileProvider},
	} {
		got, err := ParseBucketProvider(tc.input)
		if err != nil || got != tc.want {
			t.Fatalf("ParseBucketProvider(%q)=(%q,%v), want %q", tc.input, got, err, tc.want)
		}
	}
	if _, err := ParseBucketProvider("unsupported"); err == nil {
		t.Fatal("unsupported provider should fail")
	}
	if _, err := ParseBucketProvider("az"); err == nil {
		t.Fatal("az should remain a URL-scheme alias, not a provider/config value")
	}
}

func TestStorageAddressParsing(t *testing.T) {
	if got := SchemeFromURL("HTTPS://example.com"); got != "https" {
		t.Fatalf("SchemeFromURL=%q", got)
	}
	if got := BucketToURL("s3://my-bucket", "/my-key"); got != "s3://my-bucket//my-key" {
		t.Fatalf("BucketToURL=%q", got)
	}
	bucket, key, ok := ParseS3URL("s3://bucket/path/to/object")
	if !ok || bucket != "bucket" || key != "path/to/object" {
		t.Fatalf("ParseS3URL=(%q,%q,%v)", bucket, key, ok)
	}
	if _, _, ok := ParseS3URL("https://example.com"); ok {
		t.Fatal("non-s3 URL should not parse")
	}
	for _, raw := range []string{"s3://bucket", "s3://bucket/"} {
		if _, _, ok := ParseS3URL(raw); ok {
			t.Fatalf("legacy URL without a key should not parse: %q", raw)
		}
	}
	if bucket, key, ok := ParseS3URL("s3://bucket//"); !ok || bucket != "bucket" || key != "/" {
		t.Fatalf("ParseS3URL(s3://bucket//)=(%q,%q,%v), want (%q,%q,true)", bucket, key, ok, "bucket", "/")
	}
	if got, err := NormalizeStoragePath("", "bucket"); err != nil || got != "" {
		t.Fatalf("NormalizeStoragePath(empty)=(%q,%v)", got, err)
	}
	if got, err := NormalizeStoragePath("gs://bucket/a/b", "bucket"); err != nil || got != "a/b" {
		t.Fatalf("NormalizeStoragePath=(%q,%v)", got, err)
	}
	if _, err := NormalizeStoragePath("gs://other/a", "bucket"); err == nil {
		t.Fatal("mismatched path bucket should fail")
	}

	for _, tc := range []struct {
		raw, wantURL, wantScheme, wantProvider, wantBucket, wantKey, wantPath string
	}{
		{"s3://bucket/path/to/object", "s3://bucket/path/to/object", "s3", S3Provider, "bucket", "path/to/object", "/path/to/object"},
		{"s3://bucket//physical/key/", "s3://bucket//physical/key/", "s3", S3Provider, "bucket", "/physical/key/", "//physical/key/"},
		{"GS://bucket/path", "GS://bucket/path", "gs", GCSProvider, "bucket", "path", "/path"},
		{"az://bucket/path", "az://bucket/path", "az", AzureProvider, "bucket", "path", "/path"},
		{"file:///tmp/storage-root/object", "file:///tmp/storage-root/object", "file", FileProvider, "", "tmp/storage-root/object", "/tmp/storage-root/object"},
		{"/tmp/storage-root/object", "/tmp/storage-root/object", "", "", "", "tmp/storage-root/object", "/tmp/storage-root/object"},
	} {
		got, err := ParseLocation(tc.raw)
		if err != nil {
			t.Fatalf("ParseLocation(%q) error: %v", tc.raw, err)
		}
		if got.URL != tc.wantURL || got.Scheme != tc.wantScheme || got.Provider != tc.wantProvider || got.Bucket != tc.wantBucket || got.Key != tc.wantKey || got.Path != tc.wantPath {
			t.Fatalf("ParseLocation(%q)=%+v, want URL=%q scheme=%q provider=%q bucket=%q key=%q path=%q", tc.raw, got, tc.wantURL, tc.wantScheme, tc.wantProvider, tc.wantBucket, tc.wantKey, tc.wantPath)
		}
	}
	if _, err := ParseLocation("s3://bucket/%zz"); err == nil {
		t.Fatal("malformed URL escape should fail")
	}
}

func TestBucketURLRoundTripsReservedObjectKeyBytes(t *testing.T) {
	const objectKey = "dir/slash?query#fragment%literal%2F"
	gotURL := BucketToURL("bucket", objectKey)
	if want := "s3://bucket/dir/slash%3Fquery%23fragment%25literal%252F"; gotURL != want {
		t.Fatalf("BucketToURL() = %q, want %q", gotURL, want)
	}

	bucket, gotKey, ok := ParseS3URL(gotURL)
	if !ok || bucket != "bucket" || gotKey != objectKey {
		t.Fatalf("ParseS3URL() = (%q, %q, %v), want (%q, %q, true)", bucket, gotKey, ok, "bucket", objectKey)
	}

	if got := BucketToURL("bucket", "a/b"); got != "s3://bucket/a/b" {
		t.Fatalf("BucketToURL() changed ordinary URL: %q", got)
	}
}

func TestBucketURLRoundTripsLeadingAndTrailingKeySlashes(t *testing.T) {
	const objectKey = "/dir/"
	gotURL := BucketToURL("bucket", objectKey)
	if want := "s3://bucket//dir/"; gotURL != want {
		t.Fatalf("BucketToURL() = %q, want %q", gotURL, want)
	}

	bucket, gotKey, ok := ParseS3URL(gotURL)
	if !ok || bucket != "bucket" || gotKey != objectKey {
		t.Fatalf("ParseS3URL() = (%q, %q, %v), want (%q, %q, true)", bucket, gotKey, ok, "bucket", objectKey)
	}

	parsed, err := ParseLocation(gotURL)
	if err != nil || parsed.Key != objectKey {
		t.Fatalf("ParseLocation() = (%+v, %v), want key %q", parsed, err, objectKey)
	}
}

func TestTrimLeadingStoragePrefix(t *testing.T) {
	for _, tc := range []struct {
		name, key, prefix, want string
	}{
		{name: "empty", key: "", prefix: "prefix", want: ""},
		{name: "empty prefix", key: "/object/", prefix: "", want: "object"},
		{name: "both empty", key: "", prefix: "", want: ""},
		{name: "equal", key: "/prefix/", prefix: " /prefix ", want: ""},
		{name: "segment prefix", key: "prefix/object", prefix: "prefix", want: "object"},
		{name: "partial prefix", key: "prefixes/object", prefix: "prefix", want: "prefixes/object"},
		{name: "outer trim", key: " /prefix/object/ ", prefix: " /prefix/ ", want: "object"},
		{name: "repeated slashes and dots", key: "prefix//./object", prefix: "prefix", want: "/./object"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := TrimLeadingStoragePrefix(tc.key, tc.prefix); got != tc.want {
				t.Fatalf("TrimLeadingStoragePrefix(%q, %q) = %q, want %q", tc.key, tc.prefix, got, tc.want)
			}
		})
	}
}
