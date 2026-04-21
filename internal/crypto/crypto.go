package crypto

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/calypr/syfon/internal/models"
)

const (
	CredentialMasterKeyEnv    = "DRS_CREDENTIAL_MASTER_KEY"
	CredentialLocalKeyFileEnv = "DRS_CREDENTIAL_LOCAL_KEY_FILE"
	DatabaseSQLiteFileEnv     = "DRS_DB_SQLITE_FILE"
	CredentialKeyManagerEnv   = "DRS_CREDENTIAL_KEY_MANAGER"
	CredentialKMSKeyIDEnv     = "DRS_CREDENTIAL_KMS_KEY_ID"

	credentialCipherPrefixV1 = "enc:v1:"
	credentialCipherPrefixV2 = "enc:v2:"

	defaultCredentialKeyManager = "local"
	awsKMSKeyManagerName        = "aws-kms"
)

type WrappedDataKey struct {
	Manager    string
	KeyID      string
	Ciphertext string
}

type CredentialKeyManager interface {
	Name() string
	WrapDataKey(ctx context.Context, dataKey []byte) (*WrappedDataKey, error)
	UnwrapDataKey(ctx context.Context, wrapped *WrappedDataKey) ([]byte, error)
}

type credentialEnvelopeV2 struct {
	Manager    string `json:"m"`
	KeyID      string `json:"k,omitempty"`
	WrappedDEK string `json:"w"`
	Nonce      string `json:"n"`
	Ciphertext string `json:"c"`
}

type localKeyManager struct{}

func (m *localKeyManager) Name() string { return defaultCredentialKeyManager }

func (m *localKeyManager) WrapDataKey(_ context.Context, dataKey []byte) (*WrappedDataKey, error) {
	kek, err := credentialMasterKey()
	if err != nil {
		return nil, err
	}
	if len(kek) == 0 {
		return nil, fmt.Errorf("%s is required to store non-empty credentials securely", CredentialMasterKeyEnv)
	}

	nonce, ciphertext, err := encryptAESGCM(kek, dataKey)
	if err != nil {
		return nil, fmt.Errorf("wrap data key: %w", err)
	}
	payload := append(nonce, ciphertext...)
	fingerprint := sha256.Sum256(kek)
	// SECURITY FIX LOW-2: Use 16 bytes (128-bit) instead of 8 bytes for fingerprint
	return &WrappedDataKey{
		Manager:    m.Name(),
		KeyID:      "local:" + hex.EncodeToString(fingerprint[:16]),
		Ciphertext: base64.RawStdEncoding.EncodeToString(payload),
	}, nil
}

func (m *localKeyManager) UnwrapDataKey(_ context.Context, wrapped *WrappedDataKey) ([]byte, error) {
	if wrapped == nil {
		return nil, errors.New("wrapped data key is required")
	}
	kek, err := credentialMasterKey()
	if err != nil {
		return nil, err
	}
	if len(kek) == 0 {
		return nil, errors.New("encrypted credential found but master key is not configured")
	}
	payload, err := base64.RawStdEncoding.DecodeString(wrapped.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode wrapped data key: %w", err)
	}
	return decryptPackedAESGCM(kek, payload)
}

type awsKMSKeyManager struct {
	client *kms.Client
	keyID  string
}

func (m *awsKMSKeyManager) Name() string { return awsKMSKeyManagerName }

func (m *awsKMSKeyManager) WrapDataKey(ctx context.Context, dataKey []byte) (*WrappedDataKey, error) {
	if strings.TrimSpace(m.keyID) == "" {
		return nil, fmt.Errorf("%s is required for %s", CredentialKMSKeyIDEnv, awsKMSKeyManagerName)
	}
	out, err := m.client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:             aws.String(m.keyID),
		Plaintext:         dataKey,
		EncryptionContext: map[string]string{"purpose": "syfon-credential-dek"},
	})
	if err != nil {
		return nil, fmt.Errorf("kms encrypt failed: %w", err)
	}
	return &WrappedDataKey{
		Manager:    m.Name(),
		KeyID:      aws.ToString(out.KeyId),
		Ciphertext: base64.RawStdEncoding.EncodeToString(out.CiphertextBlob),
	}, nil
}

func (m *awsKMSKeyManager) UnwrapDataKey(ctx context.Context, wrapped *WrappedDataKey) ([]byte, error) {
	if wrapped == nil {
		return nil, errors.New("wrapped data key is required")
	}
	blob, err := base64.RawStdEncoding.DecodeString(wrapped.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode wrapped data key: %w", err)
	}
	out, err := m.client.Decrypt(ctx, &kms.DecryptInput{
		CiphertextBlob:    blob,
		EncryptionContext: map[string]string{"purpose": "syfon-credential-dek"},
	})
	if err != nil {
		return nil, fmt.Errorf("kms decrypt failed: %w", err)
	}
	return out.Plaintext, nil
}

var (
	credentialKeyManagerRegistryMu sync.RWMutex
	credentialKeyManagerRegistry   = map[string]func() (CredentialKeyManager, error){
		defaultCredentialKeyManager: func() (CredentialKeyManager, error) { return &localKeyManager{}, nil },
		awsKMSKeyManagerName:        newAWSKMSKeyManagerFromEnv,
	}
)

func newAWSKMSKeyManagerFromEnv() (CredentialKeyManager, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{}
	if strings.TrimSpace(os.Getenv("AWS_REGION")) == "" && strings.TrimSpace(os.Getenv("AWS_DEFAULT_REGION")) == "" {
		loadOpts = append(loadOpts, awsconfig.WithRegion("us-east-1"))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return &awsKMSKeyManager{
		client: kms.NewFromConfig(cfg),
		keyID:  strings.TrimSpace(os.Getenv(CredentialKMSKeyIDEnv)),
	}, nil
}

func RegisterCredentialKeyManager(name string, factory func() (CredentialKeyManager, error)) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return errors.New("credential key manager name is required")
	}
	if factory == nil {
		return errors.New("credential key manager factory is required")
	}
	credentialKeyManagerRegistryMu.Lock()
	defer credentialKeyManagerRegistryMu.Unlock()
	credentialKeyManagerRegistry[name] = factory
	return nil
}

func resolveCredentialKeyManager(name string) (CredentialKeyManager, error) {
	managerName := strings.ToLower(strings.TrimSpace(name))
	if managerName == "" {
		managerName = defaultCredentialKeyManager
	}
	credentialKeyManagerRegistryMu.RLock()
	factory, ok := credentialKeyManagerRegistry[managerName]
	credentialKeyManagerRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("credential key manager %q is not registered", managerName)
	}
	manager, err := factory()
	if err != nil {
		return nil, fmt.Errorf("initialize credential key manager %q: %w", managerName, err)
	}
	if manager == nil {
		return nil, fmt.Errorf("credential key manager %q returned nil", managerName)
	}
	return manager, nil
}

func configuredCredentialKeyManagerName() string {
	if name := strings.ToLower(strings.TrimSpace(os.Getenv(CredentialKeyManagerEnv))); name != "" {
		return name
	}
	if strings.TrimSpace(os.Getenv(CredentialKMSKeyIDEnv)) != "" {
		return awsKMSKeyManagerName
	}
	return defaultCredentialKeyManager
}

func credentialMasterKey() ([]byte, error) {
	raw := strings.TrimSpace(os.Getenv(CredentialMasterKeyEnv))
	if raw != "" {
		return parseUserProvidedKey(raw, CredentialMasterKeyEnv)
	}
	// Default behavior: managed local KEK persisted on the server.
	return loadOrCreateLocalCredentialKey()
}

func parseUserProvidedKey(raw string, envName string) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err == nil {
		if len(decoded) != 32 {
			return nil, fmt.Errorf("%s must decode to 32 bytes for AES-256", envName)
		}
		return decoded, nil
	}

	if len(raw) == 64 {
		hexDecoded, hexErr := hex.DecodeString(raw)
		if hexErr == nil && len(hexDecoded) == 32 {
			return hexDecoded, nil
		}
	}

	if len(raw) == 32 {
		return []byte(raw), nil
	}
	return nil, fmt.Errorf("%s must be a 32-byte raw key, 64-char hex key, or base64-encoded 32-byte key", envName)
}

func localCredentialKeyPath() string {
	if p := strings.TrimSpace(os.Getenv(CredentialLocalKeyFileEnv)); p != "" {
		return p
	}
	if sqlitePath := strings.TrimSpace(os.Getenv(DatabaseSQLiteFileEnv)); sqlitePath != "" {
		return filepath.Join(filepath.Dir(sqlitePath), ".syfon-credential-kek")
	}
	if configDir, err := os.UserConfigDir(); err == nil && strings.TrimSpace(configDir) != "" {
		return filepath.Join(configDir, "syfon", ".syfon-credential-kek")
	}
	if homeDir, err := os.UserHomeDir(); err == nil && strings.TrimSpace(homeDir) != "" {
		return filepath.Join(homeDir, ".config", "syfon", ".syfon-credential-kek")
	}
	return ".syfon-credential-kek"
}

func loadOrCreateLocalCredentialKey() ([]byte, error) {
	keyPath := localCredentialKeyPath()
	if b, err := os.ReadFile(keyPath); err == nil {
		return parseUserProvidedKey(strings.TrimSpace(string(b)), CredentialLocalKeyFileEnv)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read local credential key file %s: %w", keyPath, err)
	}

	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		return nil, fmt.Errorf("create local credential key directory for %s: %w", keyPath, err)
	}

	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("generate local credential key: %w", err)
	}
	encoded := base64.StdEncoding.EncodeToString(key) + "\n"

	f, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			b, readErr := os.ReadFile(keyPath)
			if readErr != nil {
				return nil, fmt.Errorf("read concurrent local credential key file %s: %w", keyPath, readErr)
			}
			return parseUserProvidedKey(strings.TrimSpace(string(b)), CredentialLocalKeyFileEnv)
		}
		return nil, fmt.Errorf("create local credential key file %s: %w", keyPath, err)
	}
	defer f.Close()

	if _, err := f.WriteString(encoded); err != nil {
		return nil, fmt.Errorf("write local credential key file %s: %w", keyPath, err)
	}
	return key, nil
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
	if strings.HasPrefix(plaintext, credentialCipherPrefixV1) || strings.HasPrefix(plaintext, credentialCipherPrefixV2) {
		return plaintext, nil
	}

	manager, err := resolveCredentialKeyManager(configuredCredentialKeyManagerName())
	if err != nil {
		return "", err
	}

	dek := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, dek); err != nil {
		return "", fmt.Errorf("data key generation failed: %w", err)
	}

	nonce, ciphertext, err := encryptAESGCM(dek, []byte(plaintext))
	if err != nil {
		return "", err
	}

	wrapped, err := manager.WrapDataKey(context.Background(), dek)
	if err != nil {
		return "", err
	}

	envelope := credentialEnvelopeV2{
		Manager:    wrapped.Manager,
		KeyID:      wrapped.KeyID,
		WrappedDEK: wrapped.Ciphertext,
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext),
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("encode credential envelope: %w", err)
	}
	return credentialCipherPrefixV2 + base64.RawStdEncoding.EncodeToString(b), nil
}

func DecryptCredentialField(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if !strings.HasPrefix(value, credentialCipherPrefixV1) && !strings.HasPrefix(value, credentialCipherPrefixV2) {
		return value, nil
	}

	if strings.HasPrefix(value, credentialCipherPrefixV2) {
		return decryptCredentialFieldV2(value)
	}

	// Backward compatibility for legacy v1 ciphertexts.
	return decryptCredentialFieldV1(value)
}

func decryptCredentialFieldV2(value string) (string, error) {
	payloadB64 := strings.TrimPrefix(value, credentialCipherPrefixV2)
	payload, err := base64.RawStdEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", fmt.Errorf("envelope decode failed: %w", err)
	}

	var envelope credentialEnvelopeV2
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return "", fmt.Errorf("envelope parse failed: %w", err)
	}
	if strings.TrimSpace(envelope.Manager) == "" {
		return "", errors.New("envelope manager is required")
	}

	manager, err := resolveCredentialKeyManager(envelope.Manager)
	if err != nil {
		return "", err
	}

	dek, err := manager.UnwrapDataKey(context.Background(), &WrappedDataKey{
		Manager:    envelope.Manager,
		KeyID:      envelope.KeyID,
		Ciphertext: envelope.WrappedDEK,
	})
	if err != nil {
		return "", err
	}

	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return "", fmt.Errorf("nonce decode failed: %w", err)
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return "", fmt.Errorf("ciphertext decode failed: %w", err)
	}
	plaintext, err := decryptAESGCM(dek, nonce, ciphertext)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func decryptCredentialFieldV1(value string) (string, error) {
	key, err := credentialMasterKey()
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", errors.New("encrypted credential found but master key is not configured")
	}

	payloadB64 := strings.TrimPrefix(value, credentialCipherPrefixV1)
	payload, err := base64.RawStdEncoding.DecodeString(payloadB64)
	if err != nil {
		return "", fmt.Errorf("ciphertext decode failed: %w", err)
	}
	plaintext, err := decryptPackedAESGCM(key, payload)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

func encryptAESGCM(key, plaintext []byte) ([]byte, []byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, fmt.Errorf("cipher init failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("gcm init failed: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, nil, fmt.Errorf("nonce generation failed: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return nonce, ciphertext, nil
}

func decryptAESGCM(key, nonce, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cipher init failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm init failed: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed: %w", err)
	}
	return plaintext, nil
}

func decryptPackedAESGCM(key, payload []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("cipher init failed: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("gcm init failed: %w", err)
	}
	nonceSize := gcm.NonceSize()
	if len(payload) < nonceSize {
		return nil, errors.New("ciphertext payload too short")
	}
	nonce := payload[:nonceSize]
	ciphertext := payload[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt failed: %w", err)
	}
	return plaintext, nil
}

func PrepareS3CredentialForStorage(cred *models.S3Credential) (*models.S3Credential, error) {
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

func ParseS3CredentialFromStorage(cred *models.S3Credential) (*models.S3Credential, error) {
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
