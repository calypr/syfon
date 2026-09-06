package metrics

import (
	"context"
	"strings"

	"github.com/calypr/syfon/apigen/server/metricsapi"
	"github.com/calypr/syfon/internal/usage"
	"github.com/gofiber/fiber/v3"
)

type metricsQueryContextKey struct{}

type metricsQueryParams struct {
	organization string
	program      string
	project      string
}

type MetricsServer struct {
	fileUsage      usage.FileUsageReader
	objects        usage.ObjectReader
	providerEvents usage.ProviderEventRecorder
	transferQuery  usage.TransferQuery
}

func NewMetricsServer(fileUsage usage.FileUsageReader, transferQuery usage.TransferQuery, providerEvents usage.ProviderEventRecorder, objects usage.ObjectReader) *MetricsServer {
	return &MetricsServer{
		fileUsage:      fileUsage,
		objects:        objects,
		providerEvents: providerEvents,
		transferQuery:  transferQuery,
	}
}

func RegisterMetricsRoutes(router fiber.Router, fileUsage usage.FileUsageReader, transferQuery usage.TransferQuery, providerEvents usage.ProviderEventRecorder, objects usage.ObjectReader) {
	router.Use(func(c fiber.Ctx) error {
		params := metricsQueryParams{
			organization: strings.TrimSpace(c.Query("organization")),
			program:      strings.TrimSpace(c.Query("program")),
			project:      strings.TrimSpace(c.Query("project")),
		}
		c.SetContext(context.WithValue(c.Context(), metricsQueryContextKey{}, params))
		return c.Next()
	})

	server := NewMetricsServer(fileUsage, transferQuery, providerEvents, objects)
	strict := metricsapi.NewStrictHandler(server, nil)
	metricsapi.RegisterHandlers(router, strict)
}
