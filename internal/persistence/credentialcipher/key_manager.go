package credentialcipher

import (
	"context"
)

const (
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
