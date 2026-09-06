package objects

import "testing"

func TestRecordIdentityHelpers(t *testing.T) {
	first, err := MintRecordIDFromChecksum("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"/organization/org/project/project"})
	if err != nil || first == "" {
		t.Fatalf("minted record id = %q/%v", first, err)
	}
	second, err := MintRecordIDFromChecksum("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", []string{"/programs/org/projects/project"})
	if err != nil || first != second {
		t.Fatalf("normalized identity mismatch: %q/%q/%v", first, second, err)
	}
	if _, err := MintRecordIDFromChecksum("", nil); err == nil {
		t.Fatal("expected missing checksum error")
	}
	if got := AccessMethodID(" S3 ", " s3://bucket/key "); got == "" {
		t.Fatal("expected access method id")
	}
}
