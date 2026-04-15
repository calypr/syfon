package lfs

import (
	"context"
	"net/http"
	"sync"

	"github.com/calypr/syfon/apigen/lfsapi"
	"github.com/calypr/syfon/db/core"
	"github.com/calypr/syfon/urlmanager"
	"github.com/gofiber/fiber/v3"
)

type contextKey string

const baseURLKey contextKey = "baseURL"

var (
	limitMu            sync.Mutex
	requestWindowMap   = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
)

func DefaultOptions() Options {
	return Options{
		MaxBatchObjects:              1000,
		MaxBatchBodyBytes:            10 * 1024 * 1024,
		RequestLimitPerMinute:        1200,
		BandwidthLimitBytesPerMinute: 0,
	}
}

func RegisterLFSRoutes(router fiber.Router, database core.DatabaseInterface, uM urlmanager.UrlManager, opts ...Options) {
	effective := DefaultOptions()
	if len(opts) > 0 {
		effective = opts[0]
	}

	server := NewLFSServer(database, uM, effective)
	strict := lfsapi.NewStrictHandler(server, []lfsapi.StrictMiddlewareFunc{
		lfsRequestMiddleware(effective),
	})

	router.Use(func(c fiber.Ctx) error {
		c.SetContext(context.WithValue(c.Context(), baseURLKey, c.BaseURL()))
		return c.Next()
	})

	lfsapi.RegisterHandlers(router, strict)
}

func lfsRequestMiddleware(opts Options) lfsapi.StrictMiddlewareFunc {
	return func(next lfsapi.StrictHandlerFunc, operationID string) lfsapi.StrictHandlerFunc {
		return func(ctx fiber.Ctx, args interface{}) (interface{}, error) {
			switch operationID {
			case "LfsBatch":
				if !validateLFSRequestHeaders(ctx, true, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
				if opts.MaxBatchBodyBytes > 0 && int64(len(ctx.Request().Body())) > opts.MaxBatchBodyBytes {
					writeLFSError(ctx, http.StatusRequestEntityTooLarge, "batch request body too large", false)
					return nil, nil
				}
				if req, ok := args.(lfsapi.LfsBatchRequestObject); ok && req.Body != nil {
					totalBytes := int64(0)
					for _, in := range req.Body.Objects {
						if in.Size > 0 {
							totalBytes += in.Size
						}
					}
					if !enforceBandwidthLimit(ctx, opts, totalBytes) {
						return nil, nil
					}
				}
			case "LfsStageMetadata":
				if !validateLFSRequestHeaders(ctx, false, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			case "LfsVerify":
				if !validateLFSRequestHeaders(ctx, true, true) {
					return nil, nil
				}
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			case "LfsUploadProxy":
				if !enforceRequestLimit(ctx, opts) {
					return nil, nil
				}
			}
			return next(ctx, args)
		}
	}
}

func resetLFSLimitersForTest() {
	limitMu.Lock()
	defer limitMu.Unlock()
	requestWindowMap = map[string]windowCounter{}
	bandwidthWindowMap = map[string]windowBytes{}
}
