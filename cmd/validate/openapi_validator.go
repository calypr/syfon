package validate

import (
	"context"
	"fmt"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	legacyrouter "github.com/getkin/kin-openapi/routers/legacy"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/adaptor"
)

func newDefaultSpecValidator(specPath string) (fiber.Handler, error) {
	return newSpecValidator(specPath, true)
}

func newSpecValidator(specPath string, lenientRoutes bool) (fiber.Handler, error) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true

	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("load spec %q: %w", specPath, err)
	}
	if err := spec.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("spec validation failed: %w", err)
	}

	r, err := legacyrouter.NewRouter(spec)
	if err != nil {
		return nil, fmt.Errorf("create openapi router: %w", err)
	}

	return func(c fiber.Ctx) error {
		req, err := adaptor.ConvertRequest(c, true)
		if err != nil {
			return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
				"error":  "failed to adapt request",
				"detail": err.Error(),
			})
		}

		route, pathParams, err := r.FindRoute(req)
		if err != nil {
			if lenientRoutes {
				return c.Next()
			}
			return c.Status(http.StatusNotFound).JSON(fiber.Map{
				"error":  "route not found in OpenAPI spec",
				"detail": err.Error(),
			})
		}

		in := &openapi3filter.RequestValidationInput{
			Request:    req,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				AuthenticationFunc: nil,
			},
		}
		if err := openapi3filter.ValidateRequest(req.Context(), in); err != nil {
			return c.Status(http.StatusBadRequest).JSON(fiber.Map{
				"error":  "request does not conform to OpenAPI spec",
				"detail": err.Error(),
			})
		}

		return c.Next()
	}, nil
}
