package validate

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// registerHealthzRoute adds the `/healthz` endpoint to the provided Gin router.
//
// This endpoint is typically used by load balancers, orchestration systems
// (such as Kubernetes), or monitoring tools to verify that the service is
// running and able to respond to HTTP requests.
func registerHealthzRoute(r *gin.Engine) {
	// Register a handler for HTTP GET requests on the `/healthz` path.
	// For each incoming request that matches this route, Gin will call
	// the anonymous function below.
	r.GET("/healthz", func(c *gin.Context) {
		// Respond with HTTP 200 OK and a small JSON body indicating
		// that the service is healthy. The `gin.H` type is a helper
		// for `map[string]any`, convenient for JSON responses.
		c.JSON(http.StatusOK, gin.H{
			// A simple status field; callers can check for the value
			// `"ok"` to determine that the service is healthy.
			"status": "ok",
		})
	})
}
