package buckets

import "testing"

func TestDeriveCredentialIDExcludesSecretsAndNormalizesEndpoint(t *testing.T) {
	first := DeriveCredentialID("shared-bucket", "s3", "us-east-1", "https://s3.example.org/", "access-a")
	second := DeriveCredentialID("shared-bucket", "s3", "us-east-1", "https://s3.example.org", "access-a")
	if first == "" || first != second {
		t.Fatalf("endpoint normalization changed credential id: %q != %q", first, second)
	}
	if first == DeriveCredentialID("shared-bucket", "s3", "us-east-1", "https://s3.example.org", "access-b") {
		t.Fatal("different access keys should produce distinct credential ids")
	}
}
