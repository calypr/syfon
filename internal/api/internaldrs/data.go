package internaldrs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
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

func RegisterInternalDataRoutes(router *mux.Router, database core.DatabaseInterface, uM urlmanager.UrlManager) {
	// Data routes exposed under /data to match gateway contract.
	router.Handle(config.RouteInternalDownload, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalDownload(w, r, database, uM)
	}), "InternalDownload")).Methods(http.MethodGet)

	router.Handle(config.RouteInternalUpload, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadBlank(w, r, database, uM)
	}), "InternalUploadBlank")).Methods(http.MethodPost)

	router.Handle(config.RouteInternalUploadURL, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalUploadURL(w, r, database, uM)
	}), "InternalUploadURL")).Methods(http.MethodGet)

	router.Handle(config.RouteInternalMultipartInit, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartInit(w, r, database, uM)
	}), "InternalMultipartInit")).Methods(http.MethodPost)

	router.Handle(config.RouteInternalMultipartUpload, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartUpload(w, r, database, uM)
	}), "InternalMultipartUpload")).Methods(http.MethodPost)

	router.Handle(config.RouteInternalMultipartComplete, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleInternalMultipartComplete(w, r, database, uM)
	}), "InternalMultipartComplete")).Methods(http.MethodPost)

	// Bucket endpoints.
	router.Handle(config.RouteInternalBuckets, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleInternalBuckets(w, r, database)
		case http.MethodPut:
			handleInternalPutBucket(w, r, database)
		default:
			writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
		}
	}), "InternalBuckets")).Methods(http.MethodGet, http.MethodPut)

	router.Handle(config.RouteInternalBucketDetail, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			handleInternalDeleteBucket(w, r, database)
			return
		}
		writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
	}), "InternalBucketDetail")).Methods(http.MethodDelete)

	router.Handle(config.RouteInternalBucketScopes, drs.Logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			handleInternalCreateBucketScope(w, r, database)
			return
		}
		writeHTTPError(w, r, http.StatusMethodNotAllowed, "Method not allowed", nil)
	}), "InternalBucketScopes")).Methods(http.MethodPost)
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

func resolveObjectS3Key(database core.DatabaseInterface, ctx *http.Request, objectID string, bucket string) (string, bool) {
	if strings.TrimSpace(objectID) == "" || strings.TrimSpace(bucket) == "" {
		return "", false
	}
	obj, err := resolveObjectByIDOrChecksum(database, ctx.Context(), objectID)
	if err != nil {
		return "", false
	}
	targetBucket := strings.TrimSpace(bucket)
	for _, am := range obj.AccessMethods {
		raw := strings.TrimSpace(am.AccessUrl.Url)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || !strings.EqualFold(u.Scheme, "s3") {
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
	return "", false
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
	return nil, core.ErrNotFound
}
