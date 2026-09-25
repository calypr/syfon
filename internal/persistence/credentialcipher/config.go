package credentialcipher

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	CredentialMasterKeyEnv    = "DRS_CREDENTIAL_MASTER_KEY"
	CredentialLocalKeyFileEnv = "DRS_CREDENTIAL_LOCAL_KEY_FILE"
	DatabaseSQLiteFileEnv     = "DRS_DB_SQLITE_FILE"
	CredentialKeyManagerEnv   = "DRS_CREDENTIAL_KEY_MANAGER"
	CredentialKMSKeyIDEnv     = "DRS_CREDENTIAL_KMS_KEY_ID"
)

type Config struct {
	MasterKey    string
	LocalKeyFile string
	SQLiteFile   string
	KeyManager   string
	KMSKeyID     string
}

// ConfigFromEnv reads deployment settings once at the construction boundary.
func ConfigFromEnv() Config {
	return Config{
		MasterKey:    strings.TrimSpace(os.Getenv(CredentialMasterKeyEnv)),
		LocalKeyFile: strings.TrimSpace(os.Getenv(CredentialLocalKeyFileEnv)),
		SQLiteFile:   strings.TrimSpace(os.Getenv(DatabaseSQLiteFileEnv)),
		KeyManager:   strings.TrimSpace(os.Getenv(CredentialKeyManagerEnv)),
		KMSKeyID:     strings.TrimSpace(os.Getenv(CredentialKMSKeyIDEnv)),
	}
}

func (cfg Config) localKeyPath() string {
	if p := strings.TrimSpace(cfg.LocalKeyFile); p != "" {
		return p
	}
	if p := strings.TrimSpace(cfg.SQLiteFile); p != "" {
		return filepath.Join(filepath.Dir(p), ".syfon-credential-kek")
	}
	return "/app/.syfon-credential-kek"
}

type Cipher struct {
	managerName string
	local       *localKeyManager
	managers    map[string]func() (CredentialKeyManager, error)
}

func New(cfg Config) (*Cipher, error) {
	name := strings.ToLower(strings.TrimSpace(cfg.KeyManager))
	keyID := strings.TrimSpace(cfg.KMSKeyID)
	if name == "" {
		name = defaultCredentialKeyManager
		if keyID != "" {
			name = awsKMSKeyManagerName
		}
	}
	if name != defaultCredentialKeyManager && name != awsKMSKeyManagerName {
		return nil, fmt.Errorf("credential key manager %q is not registered", name)
	}
	if name == awsKMSKeyManagerName && keyID == "" {
		return nil, fmt.Errorf("%s is required for %s", CredentialKMSKeyIDEnv, name)
	}
	local := &localKeyManager{masterKey: strings.TrimSpace(cfg.MasterKey), keyPath: cfg.localKeyPath()}
	return &Cipher{
		managerName: name,
		local:       local,
		managers: map[string]func() (CredentialKeyManager, error){
			defaultCredentialKeyManager: func() (CredentialKeyManager, error) { return local, nil },
			awsKMSKeyManagerName:        sync.OnceValues(func() (CredentialKeyManager, error) { return newAWSKMSKeyManager(keyID) }),
		},
	}, nil
}

func NewFromEnv() (*Cipher, error) { return New(ConfigFromEnv()) }

func (c *Cipher) manager(name string) (CredentialKeyManager, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	factory, ok := c.managers[name]
	if !ok {
		return nil, fmt.Errorf("credential key manager %q is not registered", name)
	}
	manager, err := factory()
	if err != nil {
		return nil, fmt.Errorf("initialize credential key manager %q: %w", name, err)
	}
	return manager, nil
}

func (c *Cipher) Enabled() (bool, error) {
	manager, err := c.manager(c.managerName)
	if err != nil {
		return false, err
	}
	if manager.Name() != defaultCredentialKeyManager {
		return true, nil
	}
	key, err := c.local.key()
	if err != nil {
		return false, err
	}
	return len(key) == 32, nil
}
