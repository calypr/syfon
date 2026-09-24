package httpapi

import (
	"encoding/json"
	"io"
	"strings"

	generated "github.com/calypr/syfon/apigen/drs"
	internalapi "github.com/calypr/syfon/apigen/internalapi"
	"github.com/calypr/syfon/internal/buckets"
	"github.com/calypr/syfon/internal/httpapi/apidocs"
	"github.com/calypr/syfon/internal/objects"
	projectstorage "github.com/calypr/syfon/internal/projects/storage"
	"github.com/calypr/syfon/internal/transfers"
	transferlfs "github.com/calypr/syfon/internal/transfers/lfs"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

const (
	RouteHealthz = "/healthz"
	RouteLivez   = "/livez"
	RouteReadyz  = "/readyz"
)

type Dependencies struct {
	ServiceInfo    generated.N200ServiceInfo
	Objects        *objects.Service
	Transfers      *transfers.Service
	LFS            *transferlfs.Service
	UsageIngest    usage.Ingestor
	UsageReports   usage.Reporter
	Buckets        *buckets.Service
	ProjectStorage *projectstorage.Service
	Authorization  fiber.Handler
	RequestIDs     fiber.Handler
	Health         *Health
}

type Options struct {
	Docs                 bool
	GA4GH                bool
	Metrics              bool
	Internal             bool
	LFS                  bool
	LFSProtocol          LFSOptions
	MaxBulkRequestLength int
}

type internalServer struct {
	objects              *objects.Service
	transfers            *transfers.Service
	projectStorage       *projectstorage.Service
	buckets              *buckets.Service
	maxBulkRequestLength int
}

func valuePointer[T any](value T) *T { return &value }

func generatedString[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}

var _ internalapi.ServerInterface = (*internalServer)(nil)

func RegisterRoutes(app fiber.Router, deps Dependencies, options Options) {
	app.Get(RouteHealthz, func(c fiber.Ctx) error {
		return c.SendString("OK")
	})
	if deps.Health != nil {
		app.Get(RouteLivez, deps.Health.live)
		app.Get(RouteReadyz, deps.Health.ready)
	}

	if !options.Docs && !options.GA4GH && !options.Metrics && !options.Internal && !options.LFS {
		return
	}

	// Documentation is intentionally registered before the protected API group.
	// The OpenAPI documents and UI are public metadata, like the health endpoints.
	if options.Docs {
		apidocs.RegisterSwaggerRoutes(app.Group("/"))
	}

	api := app.Group("/")
	var middlewares []any
	if deps.RequestIDs != nil {
		middlewares = append(middlewares, deps.RequestIDs)
	}
	if deps.Authorization != nil {
		middlewares = append(middlewares, deps.Authorization)
	}
	if len(middlewares) > 0 {
		api.Use(middlewares...)
	}

	if options.GA4GH {
		registerDRSRoutes(api.Group("/ga4gh/drs/v1"), deps.Objects, deps.Transfers, deps.ServiceInfo, options.MaxBulkRequestLength)
	}
	if options.Metrics {
		registerMetricsRoutes(api, deps.UsageReports, deps.UsageIngest)
	}
	if options.Internal {
		server := &internalServer{
			objects:              deps.Objects,
			transfers:            deps.Transfers,
			projectStorage:       deps.ProjectStorage,
			buckets:              deps.Buckets,
			maxBulkRequestLength: options.MaxBulkRequestLength,
		}
		internalapi.RegisterHandlers(api, server)
		registerBucketRoutes(api, deps.Buckets, deps.ProjectStorage)
	}
	if options.LFS {
		registerLFSRoutes(api, deps.LFS, options.LFSProtocol)
	}
}

func decodeStrictJSON(body []byte, dst any) error {
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}
