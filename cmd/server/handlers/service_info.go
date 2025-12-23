package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// registerServiceInfoRoute adds the `/service-info` endpoint to the provided
// Gin router. The endpoint returns basic metadata about the running service
// as a JSON payload, which can be used for diagnostics and observability.
func registerServiceInfoRoute(r *gin.Engine) {

	var serviceInfoResponse = map[string]any{
		"id":   "drs-example",
		"name": "Example DRS Service",
		"type": map[string]any{
			"group":    "org.ga4gh",
			"artifact": "drs",
			"version":  "1.0.0",
		},
		"description": "GA4GH DRS example service",
		"organization": map[string]any{
			"name": "Example Org",
			"url":  "https://example.org",
		},
		"contactUrl":           "mailto:support@example.org",
		"documentationUrl":     "https://example.org/docs",
		"createdAt":            "2020-01-01T00:00:00.000Z",
		"updatedAt":            "2020-01-02T00:00:00.000Z",
		"environment":          "prod",
		"version":              "1.2.3",
		"drs_version":          "1.3.0",
		"service_url":          "https://drs.example.org",
		"maxBulkRequestLength": 1000, // Deprecated
		"timestamp":            "2024-01-01T12:00:00.000Z",
		"drs": map[string]any{
			"maxBulkRequestLength":        1000,
			"objectCount":                 12345,
			"totalObjectSize":             987654321,
			"uploadRequestSupported":      true,
			"objectRegistrationSupported": true,
			"supportedUploadMethods": []string{
				"s3",
				"gs",
				"https",
			},
			"maxUploadSize":                   1099511627776,
			"maxUploadRequestLength":          100,
			"maxRegisterRequestLength":        100,
			"validateUploadChecksums":         true,
			"validateUploadFileSizes":         true,
			"relatedFileStorageSupported":     true,
			"deleteSupported":                 true,
			"maxBulkDeleteLength":             500,
			"deleteStorageDataSupported":      true,
			"accessMethodUpdateSupported":     true,
			"maxBulkAccessMethodUpdateLength": 250,
			"validateAccessMethodUpdates":     true,
		},
	}

	// Register a handler for HTTP GET requests on the `/service-info` path.
	// Gin will invoke the anonymous function for each incoming request
	// matching this route.
	r.GET("/service-info", func(c *gin.Context) {
		// Construct a JSON response using Gin's `H` helper, which is a
		// shorthand for `map[string]any`. The response includes:
		//  * `name`: a static identifier for this service.
		//  * `version`: the service version read from the SERVICE_VERSION
		//    environment variable (may be empty if not set).
		//  * `timestamp`: the current UTC time, formatted as an RFC3339Nano string.
		c.JSON(http.StatusOK, serviceInfoResponse)
	})
}
