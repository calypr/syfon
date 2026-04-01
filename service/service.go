package service

import (
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
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
