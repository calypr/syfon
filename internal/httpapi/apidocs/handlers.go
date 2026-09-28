package apidocs

import (
	"fmt"
	"log"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
	"github.com/swaggest/swgui/v5emb"
)

const (
	RouteSwaggerUI    = "/index/swagger"
	RouteSwaggerUIAlt = "/index/swagger/"
	RouteOpenAPISpec  = "/index/openapi.yaml"
	RouteLFSSpec      = "/index/openapi-lfs.yaml"
	RouteBucketSpec   = "/index/openapi-bucket.yaml"
	RouteInternalSpec = "/index/openapi-internal.yaml"
	RouteErrorSpec    = "/index/error.openapi.yaml"
)

var swaggerUIHandler = adaptor.HTTPHandler(v5emb.NewHandler("DRS Server API Docs", RouteOpenAPISpec, RouteSwaggerUI))

func handleOpenAPISpec(c fiber.Ctx) error {
	merged, err := buildMergedOpenAPISpec()
	if err != nil {
		return fmt.Errorf("OpenAPI spec file not found: %w", err)
	}
	c.Set("Content-Type", "application/yaml")
	if err := c.Send(merged); err != nil {
		log.Printf("write merged openapi spec response: %v", err)
		return err
	}
	return nil
}

func handleNamedOpenAPISpec(name, label string) fiber.Handler {
	return func(c fiber.Ctx) error {
		specBytes, err := loadSpecBytesByName(name)
		if err != nil {
			return fmt.Errorf("%s OpenAPI spec file not found: %w", label, err)
		}
		c.Set("Content-Type", "application/yaml")
		if err := c.Send(specBytes); err != nil {
			log.Printf("write %s openapi spec response: %v", strings.ToLower(label), err)
			return err
		}
		return nil
	}
}

func RegisterSwaggerRoutes(router fiber.Router) {
	router.Get(RouteSwaggerUI, swaggerUIHandler)
	router.Get(RouteSwaggerUIAlt, swaggerUIHandler)
	router.Get(RouteSwaggerUI+"/*", swaggerUIHandler)
	router.Get(RouteOpenAPISpec, handleOpenAPISpec)
	router.Get(RouteLFSSpec, handleNamedOpenAPISpec("lfs.openapi.yaml", "LFS"))
	router.Get(RouteBucketSpec, handleNamedOpenAPISpec("bucket.openapi.yaml", "Bucket"))
	router.Get(RouteInternalSpec, handleNamedOpenAPISpec("internal.openapi.yaml", "Internal"))
	router.Get(RouteErrorSpec, handleNamedOpenAPISpec("error.openapi.yaml", "Error"))
}
