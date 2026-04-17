package internaldrs

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
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

func handleInternalDownload(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/download/:file_id", func(c fiber.Ctx) error {
		return handleInternalDownloadFiber(c, om)
	})
}

func handleInternalDownloadPart(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/download/:file_id/part", func(c fiber.Ctx) error {
		return handleInternalDownloadPartFiber(c, om)
	})
}

func handleInternalUploadBlank(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload", func(c fiber.Ctx) error {
		return handleInternalUploadBlankFiber(om)(c)
	})
}

func handleInternalUploadURL(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload/:file_id", func(c fiber.Ctx) error {
		return handleInternalUploadURLFiber(om)(c)
	})
}

func handleInternalUploadBulk(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/upload/bulk", func(c fiber.Ctx) error {
		return handleInternalUploadBulkFiber(om)(c)
	})
}

func handleInternalMultipartInit(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/init", func(c fiber.Ctx) error {
		return handleInternalMultipartInitFiber(om)(c)
	})
}

func handleInternalMultipartUpload(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/upload", func(c fiber.Ctx) error {
		return handleInternalMultipartUploadFiber(om)(c)
	})
}

func handleInternalMultipartComplete(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/multipart/complete", func(c fiber.Ctx) error {
		return handleInternalMultipartCompleteFiber(om)(c)
	})
}

func handleInternalBuckets(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/buckets", func(c fiber.Ctx) error {
		return handleInternalBucketsFiber(c, om)
	})
}

func handleInternalPutBucket(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/buckets", func(c fiber.Ctx) error {
		return handleInternalPutBucketFiber(c, om)
	})
}

func handleInternalDeleteBucket(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/buckets/:bucket", func(c fiber.Ctx) error {
		return handleInternalDeleteBucketFiber(c, om)
	})
}

func handleInternalCreateBucketScope(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/data/buckets/:bucket/scopes", func(c fiber.Ctx) error {
		return handleInternalCreateBucketScopeFiber(c, om)
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

func handleInternalList(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/", func(c fiber.Ctx) error {
		return handleInternalListFiber(om)(c)
	})
}

func handleInternalCreate(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/index", func(c fiber.Ctx) error {
		return handleInternalCreateFiber(om)(c)
	})
}

func handleInternalDeleteByQuery(w http.ResponseWriter, r *http.Request, om *core.ObjectManager) {
	serveFiberHandlerHTTP(w, r, "/", func(c fiber.Ctx) error {
		return handleInternalDeleteByQueryFiber(om)(c)
	})
}

func handleInternalBulkHashes(om *core.ObjectManager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFiberHandlerHTTP(w, r, "/bulk/hashes", func(c fiber.Ctx) error {
			return handleInternalBulkHashesFiber(om)(c)
		})
	})
}

func handleInternalBulkCreate(om *core.ObjectManager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFiberHandlerHTTP(w, r, "/bulk/create", func(c fiber.Ctx) error {
			return handleInternalBulkCreateFiber(om)(c)
		})
	})
}

func handleInternalBulkDocuments(om *core.ObjectManager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFiberHandlerHTTP(w, r, "/bulk/documents", func(c fiber.Ctx) error {
			return handleInternalBulkDocumentsFiber(om)(c)
		})
	})
}
