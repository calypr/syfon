package credentials

import (
	"context"

	"github.com/calypr/syfon/client/conf"
)

type Reader interface {
	Current() *conf.Credential
}

type Manager interface {
	Reader
	Export(ctx context.Context, cred *conf.Credential) error
}
