package core

import (
	"strings"
	"testing"
)

func TestEncryptDecryptCredentialField_RoundTrip(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=") // 32 bytes base64

	ciphertext, err := EncryptCredentialField("super-secret")
	if err != nil {
		t.Fatalf("EncryptCredentialField returned error: %v", err)
	}
	if ciphertext == "super-secret" {
		t.Fatal("expected encrypted ciphertext, got plaintext")
	}
	if !strings.HasPrefix(ciphertext, "enc:v1:") {
		t.Fatalf("expected encrypted prefix, got %q", ciphertext)
	}

	plaintext, err := DecryptCredentialField(ciphertext)
	if err != nil {
		t.Fatalf("DecryptCredentialField returned error: %v", err)
	}
	if plaintext != "super-secret" {
		t.Fatalf("expected decrypted plaintext to match, got %q", plaintext)
	}
}

func TestDecryptCredentialField_LegacyPlaintext(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "")
	plaintext, err := DecryptCredentialField("legacy-plaintext")
	if err != nil {
		t.Fatalf("expected legacy plaintext support, got error: %v", err)
	}
	if plaintext != "legacy-plaintext" {
		t.Fatalf("unexpected plaintext parse result: %q", plaintext)
	}
}

func TestDecryptCredentialField_MissingKeyForEncryptedData(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
	encrypted, err := EncryptCredentialField("abc")
	if err != nil {
		t.Fatalf("encrypt setup failed: %v", err)
	}
	t.Setenv(CredentialMasterKeyEnv, "")

	_, err = DecryptCredentialField(encrypted)
	if err == nil {
		t.Fatal("expected error when decrypting encrypted data without key")
	}
}

func TestPrepareAndParseS3CredentialForStorage(t *testing.T) {
	t.Setenv(CredentialMasterKeyEnv, "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")

	cred := &S3Credential{
		Bucket:    "b",
		Provider:  "s3",
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
		Endpoint:  "https://s3.example",
	}
	stored, err := PrepareS3CredentialForStorage(cred)
	if err != nil {
		t.Fatalf("PrepareS3CredentialForStorage returned error: %v", err)
	}
	if stored.AccessKey == "ak" || stored.SecretKey == "sk" {
		t.Fatalf("expected encrypted values, got %+v", stored)
	}

	parsed, err := ParseS3CredentialFromStorage(stored)
	if err != nil {
		t.Fatalf("ParseS3CredentialFromStorage returned error: %v", err)
	}
	if parsed.AccessKey != "ak" || parsed.SecretKey != "sk" {
		t.Fatalf("expected decrypted values, got %+v", parsed)
	}
}
