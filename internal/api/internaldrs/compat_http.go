package internaldrs

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

const bucketControlResource = common.BucketControlResource

func serveFiberHandlerHTTP(w http.ResponseWriter, r *http.Request, pattern string, handler fiber.Handler) {
	app := fiber.New()
	app.Use(func(c fiber.Ctx) error {
		c.SetContext(r.Context())
		return c.Next()
	})
	switch r.Method {
	case http.MethodGet:
		app.Get(pattern, handler)
	case http.MethodPost:
		app.Post(pattern, handler)
	case http.MethodPut:
		app.Put(pattern, handler)
	case http.MethodDelete:
		app.Delete(pattern, handler)
	default:
		app.Add([]string{r.Method}, pattern, handler)
	}

	resp, err := app.Test(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer resp.Body.Close()

	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func fiberRoutePath(path string) string {
	return strings.ReplaceAll(strings.ReplaceAll(path, "{", ":"), "}", "")
}

func handleInternalDownload(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/download/:file_id", func(c fiber.Ctx) error {
		return handleInternalDownloadFiber(c, database, uM)
	})
}

func handleInternalDownloadPart(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/download/:file_id/part", func(c fiber.Ctx) error {
		return handleInternalDownloadPartFiber(c, database, uM)
	})
}

func handleInternalUploadBlank(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload", func(c fiber.Ctx) error {
		return handleInternalUploadBlankFiber(database, uM)(c)
	})
}

func handleInternalUploadURL(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload/:file_id", func(c fiber.Ctx) error {
		return handleInternalUploadURLFiber(database, uM)(c)
	})
}

func handleInternalUploadBulk(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload/bulk", func(c fiber.Ctx) error {
		return handleInternalUploadBulkFiber(database, uM)(c)
	})
}

func handleInternalMultipartInit(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/init", func(c fiber.Ctx) error {
		return handleInternalMultipartInitFiber(database, uM)(c)
	})
}

func handleInternalMultipartUpload(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/upload", func(c fiber.Ctx) error {
		return handleInternalMultipartUploadFiber(database, uM)(c)
	})
}

func handleInternalMultipartComplete(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/complete", func(c fiber.Ctx) error {
		return handleInternalMultipartCompleteFiber(database, uM)(c)
	})
}

func handleInternalBuckets(w http.ResponseWriter, r *http.Request, database db.CredentialStore) {
	serveFiberHandlerHTTP(w, r, "/data/buckets", func(c fiber.Ctx) error {
		return handleInternalBucketsFiber(c, database)
	})
}

func handleInternalPutBucket(w http.ResponseWriter, r *http.Request, database db.CredentialStore) {
	serveFiberHandlerHTTP(w, r, "/data/buckets", func(c fiber.Ctx) error {
		return handleInternalPutBucketFiber(c, database)
	})
}

func handleInternalDeleteBucket(w http.ResponseWriter, r *http.Request, database db.CredentialStore) {
	serveFiberHandlerHTTP(w, r, "/data/buckets/:bucket", func(c fiber.Ctx) error {
		return handleInternalDeleteBucketFiber(c, database)
	})
}

func handleInternalCreateBucketScope(w http.ResponseWriter, r *http.Request, database db.CredentialStore) {
	serveFiberHandlerHTTP(w, r, "/data/buckets/:bucket/scopes", func(c fiber.Ctx) error {
		return handleInternalCreateBucketScopeFiber(c, database)
	})
}

func parseScopeQuery(r *http.Request) (string, bool, error) {
	authz := strings.TrimSpace(r.URL.Query().Get("authz"))
	if authz != "" {
		return authz, true, nil
	}
	org := strings.TrimSpace(r.URL.Query().Get("organization"))
	if org == "" {
		org = strings.TrimSpace(r.URL.Query().Get("program"))
	}
	project := strings.TrimSpace(r.URL.Query().Get("project"))
	if project != "" && org == "" {
		return "", false, fmt.Errorf("organization is required when project is set")
	}
	path := common.ResourcePathForScope(org, project)
	if path != "" {
		return path, true, nil
	}
	return "", false, nil
}

func handleInternalList(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface) {
	serveFiberHandlerHTTP(w, r, "/", func(c fiber.Ctx) error {
		return handleInternalListFiber(database)(c)
	})
}

func handleInternalCreate(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface) {
	serveFiberHandlerHTTP(w, r, "/index", func(c fiber.Ctx) error {
		return handleInternalCreateFiber(database)(c)
	})
}

func handleInternalDeleteByQuery(w http.ResponseWriter, r *http.Request, database db.DatabaseInterface) {
	serveFiberHandlerHTTP(w, r, "/", func(c fiber.Ctx) error {
		return handleInternalDeleteByQueryFiber(database)(c)
	})
}

func handleInternalBulkHashes(database db.DatabaseInterface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFiberHandlerHTTP(w, r, "/bulk/hashes", func(c fiber.Ctx) error {
			return handleInternalBulkHashesFiber(database)(c)
		})
	})
}

func handleInternalBulkCreate(database db.DatabaseInterface) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFiberHandlerHTTP(w, r, "/bulk/create", func(c fiber.Ctx) error {
			return handleInternalBulkCreateFiber(database)(c)
		})
	})
}
