package validate

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/spf13/cobra"
)

// defaultSpecPath is the fallback location of the OpenAPI specification
// used to configure the request validator middleware when the environment
// variable OPENAPI_SPEC is not set.
const defaultSpecPath = "apigen/api/openapi.yaml"

// main is the entry point of the HTTP server binary.
//
// It performs the following steps:
//
//	1\) Parses CLI flags for listen address and debug logging.
//	2\) Determines the OpenAPI spec path from environment or default.
//	3\) Builds a structured logger, honoring the debug flag.
//	4\) Constructs an OpenAPI validator middleware from the spec file.
//	5\) Creates a Gin router and attaches middleware and routes.
//	6\) Starts an HTTP server with sensible timeouts.
var Cmd = &cobra.Command{
	Use:   "validate",
	Short: "Starts the validator HTTP server",
	Run: func(cmd *cobra.Command, args []string) {
		// The default is ":8080", which binds to all interfaces on port 8080.
		addr := flag.String("addr", ":8080", "listen address")

		// Define the `-debug` flag to toggle debug logging.
		// When true, the logger will emit debug\-level messages.
		debug := flag.Bool("debug", false, "enable debug logging")

		// Parse the command\-line flags and populate `addr` and `debug`.
		flag.Parse()

		// Determine the path to the OpenAPI specification from the
		// OPENAPI_SPEC environment variable, falling back to the
		// defaultSpecPath constant if the variable is not set.
		specPath := os.Getenv("OPENAPI_SPEC")
		if specPath == "" {
			specPath = defaultSpecPath
		}

		level := slog.LevelInfo
		if *debug {
			level = slog.LevelDebug
		}
		logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
		slog.SetDefault(logger)

		// Build the OpenAPI validator middleware using the resolved spec path.
		// The second argument (true) can be used to enable strict mode or
		// similar behavior, depending on newSpecValidator's implementation.
		// If the validator fails to initialize, the server cannot safely start,
		// so the process is terminated with a fatal log.
		validator, err := newSpecValidator(specPath, true)
		if err != nil {
			logger.Error("openapi validator setup failed", "err", err)
			os.Exit(1)
		}

		// Create a new Gin engine instance. Gin provides routing, middleware,
		// and HTTP handler abstractions.
		r := gin.New()

		// Attach Gin's built\-in recovery middleware. This prevents panics in
		// handlers from crashing the server by recovering and returning a 500.
		r.Use(gin.Recovery())

		// Attach the OpenAPI validator middleware so that all incoming requests
		// are validated against the OpenAPI specification before reaching the
		// actual endpoint handlers.
		r.Use(validator)

		// Register HTTP routes on the Gin engine.

		// Health endpoint: typically used by Kubernetes or other systems to
		// check if the service is alive and ready to receive traffic.
		registerHealthzRoute(r)

		// Service info endpoint: exposes basic metadata about the service,
		// such as name, version, and current timestamp.
		registerServiceInfoRoute(r)

		// Construct the HTTP server using the Gin engine as the handler.
		// ReadHeaderTimeout limits the time allowed to read request headers,
		// protecting against slow\-loris style attacks.
		srv := &http.Server{
			Addr:              *addr,
			Handler:           r,
			ReadHeaderTimeout: 5 * time.Second,
		}

		// Log that the server is starting and on which address it will listen.
		logger.Info("listening", "addr", *addr)

		// Start the HTTP server. ListenAndServe blocks until the server stops
		// or an unrecoverable error occurs. The returned error is ignored here,
		// but in a more robust setup you might log or handle it.
		if err := srv.ListenAndServe(); err != nil {
			logger.Error("validator server exited", "err", err)
		}
	},
}
