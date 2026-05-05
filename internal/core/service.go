package core

import (
	"context"
	"time"

	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/models"
	"github.com/calypr/syfon/internal/urlmanager"
)

type contextKey string

var baseURLKey contextKey = "baseURL"

const (
	objectMethodRead   = "read"
	objectMethodCreate = "create"
	objectMethodUpdate = "update"
	objectMethodDelete = "delete"
)

// WithBaseURL adds the base URL to the context.
func WithBaseURL(ctx context.Context, baseURL string) context.Context {
	return context.WithValue(ctx, baseURLKey, baseURL)
}

// GetBaseURL retrieves the base URL from the context.
func GetBaseURL(ctx context.Context) string {
	val, _ := ctx.Value(baseURLKey).(string)
	return val
}

// ObjectManager standardizes object lifecycle operations across all API surfaces.
type ObjectManager struct {
	db               db.DatabaseInterface
	uM               urlmanager.UrlManager
	bucketScopeCache *bucketScopeCache
}

type VisibleBucket struct {
	Credential models.S3Credential
	Programs   []string
}

func NewObjectManager(db db.DatabaseInterface, uM urlmanager.UrlManager) *ObjectManager {
	return &ObjectManager{
		db:               db,
		uM:               uM,
		bucketScopeCache: newBucketScopeCache(30 * time.Second),
	}
}
