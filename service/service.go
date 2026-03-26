package service

import (
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
)

// ObjectsAPIService implements the Objects API service.
type ObjectsAPIService struct {
	db         core.DatabaseInterface
	urlManager urlmanager.UrlManager
}

const (
	defaultMaxBulkRequestLength            = 200
	defaultMaxBulkDeleteLength             = 100
	defaultMaxRegisterRequestLength        = 200
	defaultMaxBulkAccessMethodUpdateLength = 200
	defaultMaxBulkChecksumAdditionLength   = 200
	defaultMaxChecksumAdditionsPerObject   = 200
)

// NewObjectsAPIService creates a new ObjectsAPIService.
func NewObjectsAPIService(db core.DatabaseInterface, urlManager urlmanager.UrlManager) *ObjectsAPIService {
	return &ObjectsAPIService{db: db, urlManager: urlManager}
}
