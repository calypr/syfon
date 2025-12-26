package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/calypr/drs-server/cmd/server/handlers"
	"github.com/calypr/drs-server/cmd/server/middleware"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// defaultSpecPath is the fallback location of the OpenAPI specification
// used to configure the request validator middleware when the environment
// variable OPENAPI_SPEC is not set.
const defaultSpecPath = "internal/apigen/api/openapi.yaml"

// main is the entry point of the HTTP server binary.
//
// It performs the following steps:
//
//	1\) Parses CLI flags for listen address and debug logging.
//	2\) Determines the OpenAPI spec path from environment or default.
//	3\) Builds a zap logger, honoring the debug flag.
//	4\) Constructs an OpenAPI validator middleware from the spec file.
//	5\) Creates a Gin router and attaches middleware and routes.
//	6\) Starts an HTTP server with sensible timeouts.
func main() {
	// Define the `-addr` flag to control the HTTP listen address.
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

	// Create a production\-oriented zap logger configuration.
	// This sets up JSON logging with reasonable defaults.
	cfg := zap.NewProductionConfig()

	// If debug logging is requested via the flag, lower the log
	// level to Debug so more verbose messages are emitted.
	if *debug {
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	// Build the actual logger from the configuration.
	// The error is intentionally ignored here for brevity, but in a
	// production system you may want to handle it explicitly.
	log, _ := cfg.Build()

	// Ensure any buffered log entries are flushed before the process exits.
	defer func() { _ = log.Sync() }()

	// Build the OpenAPI requestValidator middleware using the resolved spec path.
	// The second argument (true) can be used to enable strict mode or
	// similar behavior, depending on newSpecValidator's implementation.
	// If the requestValidator fails to initialize, the server cannot safely start,
	// so the process is terminated with a fatal log.
	requestValidator, err := middleware.NewSpecValidator(specPath, true)
	if err != nil {
		log.Fatal("openapi requestValidator", zap.Error(err))
	}

	// build the response validator middleware
	// using default config
	// If the responseValidator fails to initialize, the server cannot safely start,
	respCfg := middleware.DefaultResponseValidatorConfig()
	respCfg.Mode = middleware.ResponseValidationAudit // prod default; use Enforce in CI
	responseValidator := middleware.NewOpenAPIResponseValidator(respCfg)

	// Build the response validator middleware using the default configuration.
	// The validator runs in audit mode by default (use Enforce in CI).
	respCfg := DefaultResponseValidatorConfig()
	respCfg.Mode = ResponseValidationAudit // prod default; use Enforce in CI
	responseValidator := NewOpenAPIResponseValidator(respCfg)
	// Create a new Gin engine instance. Gin provides routing, middleware,
	// and HTTP handler abstractions.
	r := gin.New()

	// Attach Gin's built\-in recovery middleware. This prevents panics in
	// handlers from crashing the server by recovering and returning a 500.
	r.Use(gin.Recovery())

	// Attach the OpenAPI requestValidator middleware so that all incoming requests
	// are validated against the OpenAPI specification before reaching the
	// actual endpoint handlers.
	r.Use(requestValidator)
	r.Use(responseValidator)

	// Add middleware AFTER NewRouter (it returns *gin.Engine)
	//var requestLogger = RequestLogRedactingAuth()
	//r.Use(requestLogger)

	// Register HTTP routes on the Gin engine.

	// Health endpoint: typically used by Kubernetes or other systems to
	// check if the service is alive and ready to receive traffic.
	handlers.RegisterHealthzRoute(r)

	// Service info endpoint: exposes basic metadata about the service,
	// such as name, version, and current timestamp.
	handlers.RegisterServiceInfoRoute(r)

	// Construct the HTTP server using the Gin engine as the handler.
	// ReadHeaderTimeout limits the time allowed to read request headers,
	// protecting against slow\-loris style attacks.
	srv := &http.Server{
		Addr:              *addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Log that the server is starting and on which address it will listen.
	log.Info("listening", zap.String("addr", *addr))

	// Start the HTTP server. ListenAndServe blocks until the server stops
	// or an unrecoverable error occurs. The returned error is ignored here,
	// but in a more robust setup you might log or handle it.
	_ = srv.ListenAndServe()
}
