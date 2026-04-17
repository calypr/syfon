package internaldrs

import (
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/db"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

func RegisterInternalDataRoutes(router fiber.Router, database db.DatabaseInterface, uM urlmanager.UrlManager) {
	router.Get(fiberRoutePath(common.RouteInternalDownload), func(c fiber.Ctx) error { return handleInternalDownloadFiber(c, database, uM) })
	router.Get(fiberRoutePath(common.RouteInternalDownloadPart), func(c fiber.Ctx) error { return handleInternalDownloadPartFiber(c, database, uM) })
	router.Post(common.RouteInternalUpload, handleInternalUploadBlankFiber(database, uM))
	router.Get(fiberRoutePath(common.RouteInternalUploadURL), handleInternalUploadURLFiber(database, uM))
	router.Post(common.RouteInternalUploadBulk, handleInternalUploadBulkFiber(database, uM))
	router.Post(common.RouteInternalMultipartInit, handleInternalMultipartInitFiber(database, uM))
	router.Post(common.RouteInternalMultipartUpload, handleInternalMultipartUploadFiber(database, uM))
	router.Post(common.RouteInternalMultipartComplete, handleInternalMultipartCompleteFiber(database, uM))

	router.Get(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalBucketsFiber(c, database) })
	router.Put(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalPutBucketFiber(c, database) })
	router.Delete(fiberRoutePath(common.RouteInternalBucketDetail), func(c fiber.Ctx) error { return handleInternalDeleteBucketFiber(c, database) })
	router.Post(fiberRoutePath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalCreateBucketScopeFiber(c, database) })
}
