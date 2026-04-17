package internaldrs

import (
	"github.com/calypr/syfon/internal/common"
	"github.com/calypr/syfon/internal/core"
	"github.com/gofiber/fiber/v3"
)

func RegisterInternalDataRoutes(router fiber.Router, om *core.ObjectManager) {
	router.Get(fiberRoutePath(common.RouteInternalDownload), func(c fiber.Ctx) error { return handleInternalDownloadFiber(c, om) })
	router.Get(fiberRoutePath(common.RouteInternalDownloadPart), func(c fiber.Ctx) error { return handleInternalDownloadPartFiber(c, om) })
	router.Post(common.RouteInternalUpload, handleInternalUploadBlankFiber(om))
	router.Get(fiberRoutePath(common.RouteInternalUploadURL), handleInternalUploadURLFiber(om))
	router.Post(common.RouteInternalUploadBulk, handleInternalUploadBulkFiber(om))
	router.Post(common.RouteInternalMultipartInit, handleInternalMultipartInitFiber(om))
	router.Post(common.RouteInternalMultipartUpload, handleInternalMultipartUploadFiber(om))
	router.Post(common.RouteInternalMultipartComplete, handleInternalMultipartCompleteFiber(om))

	router.Get(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalBucketsFiber(c, om) })
	router.Put(common.RouteInternalBuckets, func(c fiber.Ctx) error { return handleInternalPutBucketFiber(c, om) })
	router.Delete(fiberRoutePath(common.RouteInternalBucketDetail), func(c fiber.Ctx) error { return handleInternalDeleteBucketFiber(c, om) })
	router.Post(fiberRoutePath(common.RouteInternalBucketScopes), func(c fiber.Ctx) error { return handleInternalCreateBucketScopeFiber(c, om) })
}
