package server

import (
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/docs"
	"github.com/calypr/syfon/internal/api/drsapi"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/core"
	"github.com/calypr/syfon/internal/httpapi/lfs"
	"github.com/calypr/syfon/internal/httpapi/metrics"
	"github.com/calypr/syfon/internal/httpapi/middleware"
	"github.com/calypr/syfon/internal/objects"
	"github.com/calypr/syfon/internal/transfers"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

type serverRuntime struct {
	app                 *fiber.App
	cfg                 *config.Config
	serviceInfo         drs.Service
	objectService       *objects.Service
	transferService     *transfers.Service
	usageService        *usage.Service
	om                  *core.ObjectManager
	bucketService       *buckets.Service
	authzMiddleware     *middleware.AuthzMiddleware
	requestIDMiddleware *middleware.RequestIDMiddleware
	apiGroup            fiber.Router
}

type ServerOption func(*serverRuntime)

func WithHealthzRoute() ServerOption {
	return func(rt *serverRuntime) {
		rt.app.Get(config.RouteHealthz, func(c fiber.Ctx) error {
			return c.SendString("OK")
		})
	}
}

func WithDocsRoutes() ServerOption {
	return func(rt *serverRuntime) {
		docs.RegisterSwaggerRoutes(rt.ensureAPIGroup())
	}
}

func WithGa4ghRoutes() ServerOption {
	return func(rt *serverRuntime) {
		api := rt.ensureAPIGroup().Group("/ga4gh/drs/v1")
		drsapi.RegisterDRSRoutes(api, rt.objectService, rt.transferService, rt.serviceInfo)
	}
}

func WithMetricsRoutes() ServerOption {
	return func(rt *serverRuntime) {
		metrics.RegisterMetricsRoutes(rt.ensureAPIGroup(), rt.usageService.Reports(), rt.usageService.Ingest())
	}
}

func WithInternalRoutes() ServerOption {
	return func(rt *serverRuntime) {
		api := rt.ensureAPIGroup()
		internaldrs.RegisterInternalRoutes(api, rt.objectService, rt.om, rt.transferService, rt.usageService.Ingest(), rt.bucketService)
	}
}

func WithLFSRoutes() ServerOption {
	return func(rt *serverRuntime) {
		lfs.RegisterLFSRoutes(rt.ensureAPIGroup(), lfs.Dependencies{
			ObjectService:   rt.objectService,
			TransferService: rt.transferService,
			FileCounters:    rt.usageService.Ingest(),
			Credentials:     rt.bucketService,
		}, lfs.Options{
			MaxBatchObjects:              rt.cfg.LFS.MaxBatchObjects,
			MaxBatchBodyBytes:            rt.cfg.LFS.MaxBatchBodyBytes,
			RequestLimitPerMinute:        rt.cfg.LFS.RequestLimitPerMinute,
			BandwidthLimitBytesPerMinute: rt.cfg.LFS.BandwidthLimitBytesPerMinute,
		})
	}
}

func buildServerOptions(cfg *config.Config) []ServerOption {
	opts := []ServerOption{WithHealthzRoute()}
	if cfg.Routes.Docs {
		opts = append(opts, WithDocsRoutes())
	}
	if cfg.Routes.Ga4gh {
		opts = append(opts, WithGa4ghRoutes())
	}
	if cfg.Routes.Metrics {
		opts = append(opts, WithMetricsRoutes())
	}
	if cfg.Routes.Internal {
		opts = append(opts, WithInternalRoutes())
	}
	if cfg.Routes.LFS {
		opts = append(opts, WithLFSRoutes())
	}
	return opts
}

func applyServerOptions(rt *serverRuntime, opts ...ServerOption) {
	for _, opt := range opts {
		opt(rt)
	}
}

func (rt *serverRuntime) ensureAPIGroup() fiber.Router {
	if rt.apiGroup != nil {
		return rt.apiGroup
	}
	api := rt.app.Group("/")
	api.Use(rt.requestIDMiddleware.FiberMiddleware())
	api.Use(rt.authzMiddleware.FiberMiddleware())
	rt.apiGroup = api
	return rt.apiGroup
}
