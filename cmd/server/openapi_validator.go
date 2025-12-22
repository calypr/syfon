package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/gin-gonic/gin"
)

// Convenience wrapper: default lenientRoutes to true.
func newDefaultSpecValidator(specPath string) (gin.HandlerFunc, error) {
	return newSpecValidator(specPath, true)
}

// newSpecValidator builds a Gin middleware that validates incoming requests
// against an OpenAPI 3.x document.
//
// Behavior:
// - If the request path+method does not exist in the spec => 404 (strict).
// - If it exists but the request doesn't conform => 400 with detail.
// - Security validation is not enforced unless you plug in AuthenticationFunc.
func newSpecValidator(specPath string, lenientRoutes bool) (gin.HandlerFunc, error) {
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true

	spec, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("load spec %q: %w", specPath, err)
	}

	// Validate the spec once at startup. Some specs with extensions may warn/fail;
	// adjust as needed (e.g., log warning instead of returning error).
	if err := spec.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("spec validation failed: %w", err)
	}

	// Router maps incoming HTTP requests to OpenAPI operations.
	r, err := gorillamux.NewRouter(spec)
	if err != nil {
		return nil, fmt.Errorf("create openapi router: %w", err)
	}

	return func(c *gin.Context) {
		route, pathParams, err := r.FindRoute(c.Request)
		if err != nil {
			// Lenient mode: if lenientRoutes is true, skip OpenAPI validation
			// for unknown routes and continue the handler chain.
			if lenientRoutes {
				c.Next()
				return
			}
			// Strict mode: route not in spec => 404
			c.AbortWithStatusJSON(http.StatusNotFound, gin.H{
				"error":  "route not found in OpenAPI spec",
				"detail": err.Error(),
			})
			return
		}

		// Validate request against the OpenAPI operation schema
		in := &openapi3filter.RequestValidationInput{
			Request:    c.Request,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				// If you want spec-defined security enforcement, plug in:
				// AuthenticationFunc: yourAuthFunc,
				AuthenticationFunc: nil,
			},
		}

		if err := openapi3filter.ValidateRequest(c.Request.Context(), in); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{
				"error":  "request does not conform to OpenAPI spec",
				"detail": err.Error(),
			})
			return
		}

		c.Next()
	}, nil
}
