package db

import (
	"context"

	"github.com/calypr/drs-server/apigen/drs"
)

// DatabaseInterface defines the methods required for a database backend
type DatabaseInterface interface {
	GetServiceInfo(ctx context.Context) (*drs.Service, error)
	GetObject(ctx context.Context, id string) (*drs.DrsObject, error)
	DeleteObject(ctx context.Context, id string) error
	CreateObject(ctx context.Context, obj *drs.DrsObject) error
	GetObjectsByChecksum(ctx context.Context, checksum string) ([]drs.DrsObject, error)
}
