package internaldrs

import (
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

func RegisterInternalDataRoutes(router fiber.Router, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	router.Get(fiberRoutePath(config.RouteInternalDownload), func(c fiber.Ctx) error { return handleInternalDownloadFiber(c, database, uM) })
	router.Get(fiberRoutePath(config.RouteInternalDownloadPart), func(c fiber.Ctx) error { return handleInternalDownloadPartFiber(c, database, uM) })
	router.Post(config.RouteInternalUpload, handleInternalUploadBlankFiber(database, uM))
	router.Get(fiberRoutePath(config.RouteInternalUploadURL), handleInternalUploadURLFiber(database, uM))
	router.Post(config.RouteInternalUploadBulk, handleInternalUploadBulkFiber(database, uM))
	router.Post(config.RouteInternalMultipartInit, handleInternalMultipartInitFiber(database, uM))
	router.Post(config.RouteInternalMultipartUpload, handleInternalMultipartUploadFiber(database, uM))
	router.Post(config.RouteInternalMultipartComplete, handleInternalMultipartCompleteFiber(database, uM))

	router.Get(config.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalBucketsFiber(c, database) })
	router.Put(config.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalPutBucketFiber(c, database) })
	router.Delete(fiberRoutePath(config.RouteInternalBucketDetail), func(c fiber.Ctx) error { return handleInternalDeleteBucketFiber(c, database) })
	router.Post(fiberRoutePath(config.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalCreateBucketScopeFiber(c, database) })
}
