package internaldrs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/syfon/apigen/drs"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/api/routeutil"
	"github.com/calypr/syfon/internal/provider"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

const bucketControlResource = "/services/internal/buckets"

func hasAnyMethodAccess(ctx *http.Request, resources []string, methods ...string) bool {
	if !core.IsGen3Mode(ctx.Context()) {
		return true
	}
	if len(resources) == 0 {
		return true
	}
	for _, m := range methods {
		if core.HasMethodAccess(ctx.Context(), m, resources) {
			return true
		}
	}
	return false
}

func hasGlobalBucketControlAccess(r *http.Request, methods ...string) bool {
	return hasAnyMethodAccess(r, []string{bucketControlResource}, methods...)
}

func scopeResource(org, project string) string {
	return strings.TrimSpace(core.ResourcePathForScope(org, project))
}

func hasScopedBucketAccess(r *http.Request, scope core.BucketScope, methods ...string) bool {
	res := scopeResource(scope.Organization, scope.ProjectID)
	if res == "" {
		return false
	}
	return hasAnyMethodAccess(r, []string{res}, methods...)
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

func resolveBucket(ctx *http.Request, database core.DatabaseInterface, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}

	scopes, err := database.ListBucketScopes(ctx.Context())
	if err == nil {
		for _, scope := range scopes {
			resource := scopeResource(scope.Organization, scope.ProjectID)
			if resource == "" {
				continue
			}
			if hasAnyMethodAccess(ctx, []string{resource}, "file_upload", "create", "update", "read") {
				return scope.Bucket, nil
			}
		}
	}

	creds, err := database.ListS3Credentials(ctx.Context())
	if err != nil || len(creds) == 0 {
		return "", fmt.Errorf("no bucket configured")
	}
	return creds[0].Bucket, nil
}

func resolveObjectRemotePath(database core.DatabaseInterface, ctx *http.Request, objectID string, bucket string) (string, bool) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(bucket) == "" {
		return "", false
	}
	obj, err := resolveObjectByIDOrChecksum(database, ctx.Context(), objectID)
	if err != nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	if obj.AccessMethods != nil {
		for _, am := range *obj.AccessMethods {
			if am.AccessUrl == nil {
				continue
			}
			raw := strings.TrimSpace(am.AccessUrl.Url)
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil {
				continue
			}
			// Match any supported protocol scheme.
			p := provider.FromScheme(u.Scheme)
			if p == "" {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(u.Host), targetBucket) {
				continue
			}
			key := strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
			if key != "" {
				return key, true
			}
		}
	}
	return "", false
}

func resolveObjectRemotePathWithCtx(database core.DatabaseInterface, ctx context.Context, objectID string, bucket string) (string, bool) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	return resolveObjectRemotePath(database, req, objectID, bucket)
}

func resolveObjectByIDOrChecksum(database core.DatabaseInterface, ctx context.Context, objectID string) (*core.InternalObject, error) {
	// Checksum-first resolution: internal-compatible routes are commonly called with OID.
	byChecksum, err := database.GetObjectsByChecksum(ctx, objectID)
	if err != nil {
		return nil, err
	}
	if len(byChecksum) > 0 {
		objCopy := byChecksum[0]
		return &objCopy, nil
	}

	// Legacy fallback for UUID/DID based lookups.
	obj, err := database.GetObject(ctx, objectID)
	if err == nil {
		return obj, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return nil, err
	}
	canonicalID, aliasErr := database.ResolveObjectAlias(ctx, objectID)
	if aliasErr == nil && strings.TrimSpace(canonicalID) != "" {
		obj, getErr := database.GetObject(ctx, canonicalID)
		if getErr == nil {
			objCopy := *obj
			objCopy.DrsObject.Id = objectID
			objCopy.DrsObject.SelfUri = "drs://" + objectID
			return &objCopy, nil
		}
		if !errors.Is(getErr, core.ErrNotFound) {
			return nil, getErr
		}
	}
	if aliasErr != nil && !errors.Is(aliasErr, core.ErrNotFound) {
		return nil, aliasErr
	}
	return nil, core.ErrNotFound
}
