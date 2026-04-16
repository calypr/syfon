package server

import (
	"github.com/calypr/syfon/apigen/server/drs"
	"github.com/calypr/syfon/internal/api/docs"
	"github.com/calypr/syfon/internal/api/internaldrs"
	"github.com/calypr/syfon/internal/api/lfs"
	"github.com/calypr/syfon/internal/api/metrics"
	"github.com/calypr/syfon/internal/api/middleware"
	"github.com/calypr/syfon/internal/config"
	"github.com/calypr/syfon/internal/db/core"
	"github.com/calypr/syfon/internal/service"
	"github.com/calypr/syfon/internal/urlmanager"
	"github.com/gofiber/fiber/v3"
)

type serverRuntime struct {
	app                 *fiber.App
	cfg                 *config.Config
	database            core.DatabaseInterface
	uM                  urlmanager.UrlManager
	svc                 *service.ObjectsAPIService
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
		if rt.svc == nil {
			return
		}
		strict := service.NewStrictServer(rt.svc)
		drsServer := drs.NewStrictHandler(strict, nil)
		drs.RegisterHandlersWithOptions(rt.ensureAPIGroup(), drsServer, drs.FiberServerOptions{
			BaseURL: "/ga4gh/drs/v1",
		})
	}
}

func WithMetricsRoutes() ServerOption {
	return func(rt *serverRuntime) {
		metrics.RegisterMetricsRoutes(rt.ensureAPIGroup(), rt.database)
	}
}

func WithInternalRoutes() ServerOption {
	return func(rt *serverRuntime) {
		api := rt.ensureAPIGroup()
		internaldrs.RegisterInternalIndexRoutes(api, rt.database, rt.uM)
		internaldrs.RegisterInternalDataRoutes(api, rt.database, rt.uM)
	}
}

func WithLFSRoutes() ServerOption {
	return func(rt *serverRuntime) {
		lfs.RegisterLFSRoutes(rt.ensureAPIGroup(), rt.database, rt.uM, lfs.Options{
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
