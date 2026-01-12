package validate

import (
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
)

// registerServiceInfoRoute adds the `/service-info` endpoint to the provided
// Gin router. The endpoint returns basic metadata about the running service
// as a JSON payload, which can be used for diagnostics and observability.
func registerServiceInfoRoute(r *gin.Engine) {
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
		c.JSON(http.StatusOK, gin.H{
			// `name` identifies the service. This is currently hard-coded but could
			// be wired to build\-time variables in the future.
			"name": "my-service",

			// `version` is derived from the SERVICE_VERSION environment variable,
			// allowing deployments to inject the build or release version.
			"version": os.Getenv("SERVICE_VERSION"),

			// `timestamp` indicates when the response was generated, using UTC and
			// RFC3339Nano for high-precision, machine\-readable timestamps.
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
	})
}
