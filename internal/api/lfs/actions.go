package lfs

import (
	"context"

	"github.com/calypr/syfon/apigen/server/lfsapi"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/urlmanager"
)

func prepareDownloadActions(ctx context.Context, database db.LFSStore, uM urlmanager.UrlManager, oid string) (*lfsapi.BatchActions, *lfsapi.ObjectError) {
	return service.PrepareDownloadActions(ctx, database, uM, oid)
}

func prepareUploadActions(ctx context.Context, database db.LFSStore, uM urlmanager.UrlManager, oid string, reqSize int64, baseURL string) (*lfsapi.BatchActions, int64, *lfsapi.ObjectError) {
	return service.PrepareUploadActions(ctx, database, uM, oid, reqSize, baseURL)
}

func proxySinglePut(ctx context.Context, uM urlmanager.UrlManager, bucket, key string) error {
	return service.ProxySinglePut(ctx, uM, bucket, key)
}

func uploadPartToSignedURL(ctx context.Context, signedURL string, content []byte) (string, error) {
	return service.UploadPartToSignedURL(ctx, signedURL, content)
}
