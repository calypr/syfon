package internaldrs

import (
	"context"
	"net/http"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/api/internaldrs/logic"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

const bucketControlResource = logic.BucketControlResource

func hasAnyMethodAccess(ctx *http.Request, resources []string, methods ...string) bool {
	return logic.HasAnyMethodAccess(ctx, resources, methods...)
}

func hasGlobalBucketControlAccess(r *http.Request, methods ...string) bool {
	return logic.HasGlobalBucketControlAccess(r, methods...)
}

func scopeResource(org, project string) string {
	return logic.ScopeResource(org, project)
}

func hasScopedBucketAccess(r *http.Request, scope core.BucketScope, methods ...string) bool {
	return logic.HasScopedBucketAccess(r, scope, methods...)
}

func RegisterInternalDataRoutes(router fiber.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	router.Get(routeutil.FiberPath(config.RouteInternalDownload), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDownload(w, r, database, uM)
	}), "InternalDownload"), "file_id"))
	router.Get(routeutil.FiberPath(config.RouteInternalDownloadPart), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDownloadPart(w, r, database, uM)
	}), "InternalDownloadPart"), "file_id"))
	router.Post(routeutil.FiberPath(config.RouteInternalUpload), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadBlank(w, r, database, uM)
	}), "InternalUploadBlank")))
	router.Get(routeutil.FiberPath(config.RouteInternalUploadURL), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadURL(w, r, database, uM)
	}), "InternalUploadURL"), "file_id"))
	router.Post(routeutil.FiberPath(config.RouteInternalUploadBulk), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadBulk(w, r, database, uM)
	}), "InternalUploadBulk")))
	router.Post(routeutil.FiberPath(config.RouteInternalMultipartInit), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartInit(w, r, database, uM)
	}), "InternalMultipartInit")))
	router.Post(routeutil.FiberPath(config.RouteInternalMultipartUpload), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartUpload(w, r, database, uM)
	}), "InternalMultipartUpload")))
	router.Post(routeutil.FiberPath(config.RouteInternalMultipartComplete), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartComplete(w, r, database, uM)
	}), "InternalMultipartComplete")))

	router.Get(routeutil.FiberPath(config.RouteInternalBuckets), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalBuckets(w, r, database)
	}), "InternalBuckets")))
	router.Put(routeutil.FiberPath(config.RouteInternalBuckets), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalPutBucket(w, r, database)
	}), "InternalBuckets")))
	router.Delete(routeutil.FiberPath(config.RouteInternalBucketDetail), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDeleteBucket(w, r, database)
	}), "InternalBucketDetail"), "bucket"))
	router.Post(routeutil.FiberPath(config.RouteInternalBucketScopes), routeutil.Handler(drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalCreateBucketScope(w, r, database)
	}), "InternalBucketScopes"), "bucket"))
}

func resolveBucket(ctx *http.Request, database core.CredentialStore, requested string) (string, error) {
	return logic.ResolveBucket(ctx, database, requested)
}

func resolveObjectRemotePath(database core.ObjectStore, ctx *http.Request, objectID string, bucket string) (string, bool) {
	return logic.ResolveObjectRemotePath(database, ctx, objectID, bucket)
}

func resolveObjectRemotePathWithCtx(database core.ObjectStore, ctx context.Context, objectID string, bucket string) (string, bool) {
	return logic.ResolveObjectRemotePathWithCtx(database, ctx, objectID, bucket)
}

func resolveObjectByIDOrChecksum(database core.ObjectStore, ctx context.Context, objectID string) (*core.InternalObject, error) {
	return logic.ResolveObjectByIDOrChecksum(database, ctx, objectID)
}
