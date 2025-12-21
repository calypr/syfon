// internal/apigen/openapi_validator.go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	sw "github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/legacy"
	"github.com/gin-gonic/gin"
)

func newSpecValidator(specPath string) (gin.HandlerFunc, error) {
	loader := &openapi3.Loader{IsExternalRefsAllowed: true}
	doc, err := loader.LoadFromFile(specPath)
	if err != nil {
		return nil, fmt.Errorf("load OpenAPI spec: %w", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		return nil, fmt.Errorf("validate OpenAPI spec: %w", err)
	}

	oasRouter, err := legacy.NewRouter(doc)
	if err != nil {
		return nil, fmt.Errorf("build OpenAPI router: %w", err)
	}

	return func(c *gin.Context) {
		req := c.Request.Clone(c.Request.Context())

		if req.Body != nil {
			bodyBytes, err := io.ReadAll(req.Body)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": "unable to read request body"})
				return
			}
			req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			req.ContentLength = int64(len(bodyBytes))
			c.Request.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			c.Request.ContentLength = int64(len(bodyBytes))
		}

		route, pathParams, err := oasRouter.FindRoute(req)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		input := &openapi3filter.RequestValidationInput{
			Request:    req,
			PathParams: pathParams,
			Route:      route,
			Options: &openapi3filter.Options{
				AuthenticationFunc: openapi3filter.NoopAuthenticationFunc,
			},
		}
		if err := openapi3filter.ValidateRequest(c.Request.Context(), input); err != nil {
			c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.Next()
	}, nil
}
