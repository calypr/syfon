package core

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	CredentialMasterKeyEnv = "DRS_CREDENTIAL_MASTER_KEY"
	credentialCipherPrefix = "enc:v1:"
)

func credentialMasterKey() ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv(CredentialMasterKeyEnv))
	if raw == "" {
		return nil, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err == nil {
		if len(decoded) != 32 {
			return nil, fmt.Errorf("%s must decode to 32 bytes for AES-256", CredentialMasterKeyEnv)
		}
		return decoded, nil
	}

	if len(raw) == 32 {
		return []byte(raw), nil
	}
	return nil, fmt.Errorf("%s must be a 32-byte raw key or base64-encoded 32-byte key", CredentialMasterKeyEnv)
}

func CredentialEncryptionEnabled() (bool, error) {
	key, err := credentialMasterKey()
	if err != nil {
		return false, err
	}
	return len(key) == 32, nil
}

func EncryptCredentialField(plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if strings.HasPrefix(plaintext, credentialCipherPrefix) {
		return plaintext, nil
	}

	key, err := credentialMasterKey()
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", fmt.Errorf("%s is required to store non-empty credentials securely", CredentialMasterKeyEnv)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("cipher init failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm init failed: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("nonce generation failed: %w", err)
	}

	ciphertext := gcm.Seal(nil, nonce, []byte(plaintext), nil)
	payload := append(nonce, ciphertext...)
	return credentialCipherPrefix + base64.RawStdEncoding.EncodeToString(payload), nil
}

func DecryptCredentialField(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, credentialCipherPrefix) {
		return value, nil
	}

	key, err := credentialMasterKey()
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", errors.New("encrypted credential found but master key is not configured")
	}

	payloadB64 := strings.TrimPrefix(value, credentialCipherPrefix)
	payload, err := base64.RawStdEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", fmt.Errorf("ciphertext decode failed: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("cipher init failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("gcm init failed: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(payload) < nonceSize {
		return "", errors.New("ciphertext payload too short")
	}
	nonce := payload[:nonceSize]
	ciphertext := payload[nonceSize:]

	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt failed: %w", err)
	}
	return string(plaintext), nil
}

func PrepareS3CredentialForStorage(cred *S3Credential) (*S3Credential, error) {
	if cred == nil {
		return nil, errors.New("credential is required")
	}
	out := *cred
	var err error
	out.AccessKey, err = EncryptCredentialField(out.AccessKey)
	if err != nil {
		return nil, err
	}
	out.SecretKey, err = EncryptCredentialField(out.SecretKey)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func ParseS3CredentialFromStorage(cred *S3Credential) (*S3Credential, error) {
	if cred == nil {
		return nil, errors.New("credential is required")
	}
	out := *cred
	var err error
	out.AccessKey, err = DecryptCredentialField(out.AccessKey)
	if err != nil {
		return nil, err
	}
	out.SecretKey, err = DecryptCredentialField(out.SecretKey)
	if err != nil {
		return nil, err
	}
	return &out, nil
}
