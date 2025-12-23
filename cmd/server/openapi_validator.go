package main

import (
	"context"
	"fmt"
	"log"
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

	//
	if len(spec.Servers) > 0 {
		for _, server := range spec.Servers {
			log.Printf("Loaded OpenAPI server URL: %s", server.URL)
		}
		spec.Servers = openapi3.Servers{{URL: "/"}} // accept any host, root base path
		log.Printf("Neutralize servers so route matching isn’t constrained e.g. /")
	}
	// Log all paths loaded from the spec for visibility.
	if spec.Paths != nil {
		for path := range spec.Paths.Map() {
			log.Printf("Loaded OpenAPI path: %s", path)
		}
	}
	// spot check service-info
	if pi := spec.Paths.Find("/service-info"); pi != nil {
		log.Printf("/service-info GET defined? %v", pi.Get != nil)
		log.Printf("/service-info POST defined? %v", pi.Post != nil)
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
				log.Printf("error finding route %s, but lenientRoutes - proceeding: %v", c.Request.URL.Path, err)
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

		// Attach route and path params to context for downstream handlers
		c.Set("oapi.route", route)
		c.Set("oapi.pathParams", pathParams)

		// Validate request against the OpenAPI operation schema
		in := &openapi3filter.RequestValidationInput{
			Request:    c.Request,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				// If you want spec-defined security enforcement, plug in:
				// AuthenticationFunc: yourAuthFunc,
				AuthenticationFunc: allowAllAuth,
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

// Replace this with real auth logic (e.g., checking Authorization headers, API keys, etc.).
func allowAllAuth(ctx context.Context, input *openapi3filter.AuthenticationInput) error {
	// input.SecurityScheme and input.Scopes describe what the spec requires.
	// Implement your checks here. Return nil on success, or an error on failure.
	return nil
}
