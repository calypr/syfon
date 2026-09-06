package common

import (
	"testing"

	"github.com/calypr/syfon/internal/objects"
)

func TestParseHashQuery(t *testing.T) {
	type tc struct {
		rawHash string
		rawType string
		wantT   string
		wantV   string
	}
	cases := []tc{
		{rawHash: "sha256:ABC", rawType: "", wantT: "sha256", wantV: "ABC"},
		{rawHash: "  'abc'  ", rawType: "sha-256", wantT: "sha256", wantV: "abc"},
		{rawHash: "value", rawType: "md5", wantT: "md5", wantV: "value"},
	}
	for _, c := range cases {
		gotT, gotV := objects.ParseHashQuery(c.rawHash, c.rawType)
		if gotT != c.wantT || gotV != c.wantV {
			t.Fatalf("ParseHashQuery(%q,%q) got (%q,%q) want (%q,%q)", c.rawHash, c.rawType, gotT, gotV, c.wantT, c.wantV)
		}
	}
}

func TestMergeAdditionalChecksums(t *testing.T) {
	existing := []objects.Checksum{{Type: "sha256", Checksum: "a"}}
	additions := []objects.Checksum{
		{Type: "sha-256", Checksum: "b"}, // duplicate type, ignored
		{Type: "md5", Checksum: "m"},
		{Type: "", Checksum: "x"},
	}
	out := objects.MergeAdditionalChecksums(existing, additions)
	if len(out) != 2 {
		t.Fatalf("expected 2 checksums, got %+v", out)
	}
	if out[1].Type != "md5" || out[1].Checksum != "m" {
		t.Fatalf("unexpected merged checksum %+v", out[1])
	}
}

func TestSHAHelpers(t *testing.T) {
	if !objects.LooksLikeSHA256("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") {
		t.Fatalf("expected valid sha256 format")
	}
	if objects.LooksLikeSHA256("abc") {
		t.Fatalf("expected invalid sha256 format")
	}

	checksums := []objects.Checksum{{Type: "sha-256", Checksum: " AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA "}}
	sha, ok := objects.CanonicalSHA256(checksums)
	if !ok || sha != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("CanonicalSHA256 mismatch: ok=%v sha=%q", ok, sha)
	}

	obj := objects.Record{Checksums: []objects.Checksum{{Type: "md5", Checksum: "m"}}}
	if !objects.RecordHasChecksumTypeAndValue(obj, "md5", "m") {
		t.Fatalf("expected checksum match")
	}
}

func TestParseS3URL(t *testing.T) {
	bucket, key, ok := ParseS3URL("s3://bucket/path/to/object")
	if !ok || bucket != "bucket" || key != "path/to/object" {
		t.Fatalf("ParseS3URL unexpected output: ok=%v bucket=%q key=%q", ok, bucket, key)
	}
	if _, _, ok := ParseS3URL("https://example.com"); ok {
		t.Fatalf("expected non-s3 URL to fail")
	}
}
