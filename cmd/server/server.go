package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/calypr/drs-server/apigen/drs"
	"github.com/calypr/drs-server/config"
	"github.com/calypr/drs-server/db/core"
	"github.com/calypr/drs-server/db/postgres"
	"github.com/calypr/drs-server/db/sqlite"
	coreapi "github.com/calypr/drs-server/internal/api/coreapi"
	"github.com/calypr/drs-server/internal/api/docs"
	"github.com/calypr/drs-server/internal/api/internaldrs"
	"github.com/calypr/drs-server/internal/api/lfs"
	"github.com/calypr/drs-server/internal/api/metrics"
	"github.com/calypr/drs-server/internal/api/middleware"
	"github.com/calypr/drs-server/service"
	"github.com/calypr/drs-server/urlmanager"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
)

var configFile string

var Cmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the DRS Object API server",
	Run: func(cmd *cobra.Command, args []string) {
		// Load Config
		cfg, err := config.LoadConfig(configFile)
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}
		if cfg.Auth.Mode == config.AuthModeGen3 && cfg.Database.Postgres == nil && !isMockAuthEnabled() {
			log.Fatal("auth.mode=gen3 requires postgres database")
		}

		// Init DB
		var database core.DatabaseInterface
		var errDb error

		if cfg.Database.Sqlite != nil {
			dbPath := cfg.Database.Sqlite.File
			if dbPath == "" {
				dbPath = "drs.db"
			}
			fmt.Printf("Initializing SqliteDB (File: %s)\n", dbPath)
			database, errDb = sqlite.NewSqliteDB(dbPath)
		} else if cfg.Database.Postgres != nil {
			dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
				cfg.Database.Postgres.User,
				cfg.Database.Postgres.Password,
				cfg.Database.Postgres.Host,
				cfg.Database.Postgres.Port,
				cfg.Database.Postgres.Database,
				cfg.Database.Postgres.SSLMode,
			)
			fmt.Printf("Initializing PostgresDB (Host: %s, DB: %s)\n", cfg.Database.Postgres.Host, cfg.Database.Postgres.Database)
			database, errDb = postgres.NewPostgresDB(dsn)
		} else {
			log.Fatal("No database configuration provided")
		}

		if errDb != nil {
			log.Fatalf("Failed to initialize database: %v", errDb)
		}

		// Load S3 Credentials from Config if present
		if len(cfg.S3Credentials) > 0 {
			fmt.Printf("Loading %d S3 credentials from config\n", len(cfg.S3Credentials))
			for _, c := range cfg.S3Credentials {
				cred := &core.S3Credential{
					Bucket:    c.Bucket,
					Provider:  c.Provider,
					Region:    c.Region,
					AccessKey: c.AccessKey,
					SecretKey: c.SecretKey,
					Endpoint:  c.Endpoint,
				}
				if err := database.SaveS3Credential(cmd.Context(), cred); err != nil {
					log.Printf("Failed to save credential for bucket %s: %v", c.Bucket, err)
				}
			}
		}

		// Init unified URL manager.
		uM := urlmanager.NewManager(database, cfg.Signing)

		// Init Service
		service := service.NewObjectsAPIService(database, uM)

		// Init Controller
		objectsController := drs.NewObjectsAPIController(service)
		serviceInfoController := drs.NewServiceInfoAPIController(service)
		uploadRequestController := drs.NewUploadRequestAPIController(service)

		// Init Router (register generated routes by specificity to avoid path shadowing:
		// e.g. /objects/register must match before /objects/{object_id}).
		router := mux.NewRouter().StrictSlash(true)
		registerAPIRoutes(router, objectsController, serviceInfoController, uploadRequestController)

		// Init AuthZ Middleware
		// We use a standard slog.Logger for data-client compatibility
		slogLogger := slog.New(slog.NewTextHandler(log.Writer(), &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(slogLogger)
		authzMiddleware := middleware.NewAuthzMiddleware(
			slogLogger,
			cfg.Auth.Mode,
			cfg.Auth.Basic.Username,
			cfg.Auth.Basic.Password,
		)
		requestIDMiddleware := middleware.NewRequestIDMiddleware(slogLogger)

		// Apply Middlewares
		router.Use(requestIDMiddleware.Middleware)
		router.Use(authzMiddleware.Middleware)

		router.HandleFunc(config.RouteHealthz, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})

		docs.RegisterSwaggerRoutes(router)
		coreapi.RegisterCoreRoutes(router, database)
		metrics.RegisterMetricsRoutes(router, database)

		internaldrs.RegisterInternalIndexRoutes(router, database)

		internaldrs.RegisterInternalDataRoutes(router, database, uM)
		fmt.Println("Internal DRS compatibility routes enabled")

		// Register Git LFS API routes
		lfs.RegisterLFSRoutes(router, database, uM, lfs.Options{
			MaxBatchObjects:              cfg.LFS.MaxBatchObjects,
			MaxBatchBodyBytes:            cfg.LFS.MaxBatchBodyBytes,
			RequestLimitPerMinute:        cfg.LFS.RequestLimitPerMinute,
			BandwidthLimitBytesPerMinute: cfg.LFS.BandwidthLimitBytesPerMinute,
		})

		addr := fmt.Sprintf(":%d", cfg.Port)
		fmt.Printf("Server starting on %s\n", addr)
		server := &http.Server{
			Addr:              addr,
			Handler:           router,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      120 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		errCh := make(chan error, 1)
		go func() {
			if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigCh)

		select {
		case err := <-errCh:
			log.Fatalf("server listen failed: %v", err)
		case sig := <-sigCh:
			slog.Info("shutdown signal received", "signal", sig.String())
		case <-cmd.Context().Done():
			slog.Info("shutdown requested by context cancellation")
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Fatalf("server shutdown failed: %v", err)
		}
		slog.Info("server shutdown complete")
	},
}

func init() {
	Cmd.Flags().StringVar(&configFile, "config", "", "Path to configuration file (json/yaml)")
}

func isMockAuthEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("DRS_AUTH_MOCK_ENABLED"))
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func registerControllerRoutes(router *mux.Router, api drs.Router) {
	registerSortedRoutes(router, append([]drs.Route(nil), api.OrderedRoutes()...))
}

func registerAPIRoutes(router *mux.Router, apis ...drs.Router) {
	routes := make([]drs.Route, 0)
	for _, api := range apis {
		routes = append(routes, api.OrderedRoutes()...)
	}
	registerSortedRoutes(router, routes)
}

func registerSortedRoutes(router *mux.Router, routes []drs.Route) {
	sort.SliceStable(routes, func(i, j int) bool {
		a := routes[i]
		b := routes[j]
		aParams := strings.Count(a.Pattern, "{")
		bParams := strings.Count(b.Pattern, "{")
		if aParams != bParams {
			return aParams < bParams
		}
		aSegs := segmentCount(a.Pattern)
		bSegs := segmentCount(b.Pattern)
		if aSegs != bSegs {
			return aSegs > bSegs
		}
		return len(a.Pattern) > len(b.Pattern)
	})

	for _, route := range routes {
		var handler http.Handler = route.HandlerFunc
		handler = drs.Logger(handler, route.Name)
		router.Methods(route.Method).Path(route.Pattern).Name(route.Name).Handler(handler)
	}
}

func segmentCount(pattern string) int {
	trimmed := strings.Trim(pattern, "/")
	if trimmed == "" {
		return 0
	}
	return strings.Count(trimmed, "/") + 1
}
